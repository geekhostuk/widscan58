/* GPL-3.0-or-later. */
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#if LINK_UART
#include "driver/uart.h"
#else
#include "driver/usb_serial_jtag.h"
#endif
#include "esp_attr.h"
#include "esp_rom_crc.h"
#include "freertos/FreeRTOS.h"
#include "freertos/ringbuf.h"
#include "freertos/task.h"
#include "frames.h"
#include "link.h"

#if RX_TARGET_C5
/* The C5 heap is ~125 KB once the 128 KiB capture bank is reserved, and Wi-Fi
 * needs much of what's left. RAWIQ goes out in chunks, so the queue only has
 * to hold a reply (3 KB) and ride out short host stalls. */
#define LINK_QUEUE_BYTES (16 * 1024)
#define LINK_USB_TX_BYTES 4096
#else
#define LINK_QUEUE_BYTES (72 * 1024)
#define LINK_USB_TX_BYTES 16384
#endif
#if LINK_UART
/* LINK_UART: the link runs over a hardware UART instead of USB Serial/JTAG,
 * for a host that can't be a USB host (a gate controller). Pins and baud come
 * from the build (platformio.ini, env esp32c5_uart). */
#ifndef LINK_UART_NUM
#define LINK_UART_NUM UART_NUM_1
#endif
#ifndef LINK_UART_BAUD
#define LINK_UART_BAUD 2000000
#endif
#define LINK_RX_BYTES 1024

static int hw_write(const uint8_t *p, size_t n, TickType_t wait) {
    (void)wait; /* blocks until the bytes are in the driver's TX buffer */
    return uart_write_bytes(LINK_UART_NUM, p, n);
}

static void hw_init(void) {
    uart_config_t cfg = {
        .baud_rate = LINK_UART_BAUD,
        .data_bits = UART_DATA_8_BITS,
        .parity = UART_PARITY_DISABLE,
        .stop_bits = UART_STOP_BITS_1,
        .flow_ctrl = UART_HW_FLOWCTRL_DISABLE,
        .source_clk = UART_SCLK_DEFAULT,
    };
    ESP_ERROR_CHECK(uart_driver_install(LINK_UART_NUM, LINK_RX_BYTES, LINK_USB_TX_BYTES, 0, NULL, 0));
    ESP_ERROR_CHECK(uart_param_config(LINK_UART_NUM, &cfg));
    ESP_ERROR_CHECK(uart_set_pin(LINK_UART_NUM, LINK_UART_TX, LINK_UART_RX, UART_PIN_NO_CHANGE, UART_PIN_NO_CHANGE));
}

/* As usb_serial_jtag_read_bytes: wait for the first byte only, then take
 * what has arrived (uart_read_bytes alone waits for all of capacity). */
static int hw_read(uint8_t *buf, size_t capacity, TickType_t wait) {
    if (!capacity) return 0;
    int n = uart_read_bytes(LINK_UART_NUM, buf, 1, wait);
    if (n <= 0) return n;
    size_t more = 0;
    uart_get_buffered_data_len(LINK_UART_NUM, &more);
    if (more > capacity - 1) more = capacity - 1;
    if (more) {
        int m = uart_read_bytes(LINK_UART_NUM, buf + 1, more, 0);
        if (m > 0) n += m;
    }
    return n;
}
#else
static int hw_write(const uint8_t *p, size_t n, TickType_t wait) { return usb_serial_jtag_write_bytes(p, n, wait); }

static void hw_init(void) {
    usb_serial_jtag_driver_config_t cfg = {.tx_buffer_size = LINK_USB_TX_BYTES, .rx_buffer_size = 1024};
    ESP_ERROR_CHECK(usb_serial_jtag_driver_install(&cfg));
}

static int hw_read(uint8_t *buf, size_t capacity, TickType_t wait) {
    return usb_serial_jtag_read_bytes(buf, capacity, wait);
}
#endif

static RingbufHandle_t queue;
static volatile uint32_t drops;
static volatile bool streaming; /* frames allowed (link_set_streaming); off at boot */

static void link_task(void *arg) {
    (void)arg;
    for (;;) {
        size_t size;
        uint8_t *item = xRingbufferReceive(queue, &size, portMAX_DELAY);
        if (!item) continue;
        /* With no host attached the USB FIFO never drains; drop quickly. */
        size_t done = 0;
        while (done < size) {
            /* The driver cannot accept a write larger than its TX buffer. */
            size_t chunk = size - done > 4096 ? 4096 : size - done;
            int n = hw_write(item + done, chunk, pdMS_TO_TICKS(50));
            if (n <= 0) break;
            done += (size_t)n;
        }
        if (done < size) drops++;
        vRingbufferReturnItem(queue, item);
    }
}

void link_init(void) {
    hw_init();
    queue = xRingbufferCreate(LINK_QUEUE_BYTES, RINGBUF_TYPE_NOSPLIT);
    xTaskCreatePinnedToCore(link_task, "link", 4096, NULL, 5, NULL, portNUM_PROCESSORS - 1);
}

size_t link_capacity(void) { return xRingbufferGetMaxItemSize(queue); }
uint32_t link_drops(void) { return drops; }
unsigned link_fill_permille(void) {
    /* A no-split ring reports at most its largest possible item as free. */
    size_t free = xRingbufferGetCurFreeSize(queue), max = xRingbufferGetMaxItemSize(queue);
    return free >= max ? 0 : (unsigned)(1000 - free * 1000 / max);
}

static IRAM_ATTR bool frame_put(uint8_t type, const void *a, size_t alen, const void *b, size_t blen, size_t min_free,
                      TickType_t wait) {
    if (!streaming) return false; /* not a drop: nobody is listening for frames */
    size_t len = alen + blen;
    size_t total = sizeof(frame_header_t) + len + 4;
    if (len > 0xffff || (min_free && xRingbufferGetCurFreeSize(queue) < total + min_free)) {
        drops++;
        return false;
    }
    void *slot;
    if (xRingbufferSendAcquire(queue, &slot, total, wait) != pdTRUE) {
        drops++;
        return false;
    }
    uint8_t *p = slot;
    frame_header_t h = {FRAME_SYNC0, FRAME_SYNC1, type, PROTO_VERSION, (uint16_t)len};
    memcpy(p, &h, sizeof(h));
    if (alen) memcpy(p + sizeof(h), a, alen);
    if (blen) memcpy(p + sizeof(h) + alen, b, blen);
    uint32_t crc = esp_rom_crc32_le(0, p, sizeof(h) + len);
    memcpy(p + sizeof(h) + len, &crc, 4);
    xRingbufferSendComplete(queue, slot);
    return true;
}

IRAM_ATTR bool link_frame(uint8_t type, const void *a, size_t alen, const void *b, size_t blen, size_t min_free) {
    return frame_put(type, a, alen, b, blen, min_free, 0);
}

bool link_frame_wait(uint8_t type, const void *a, size_t alen, const void *b, size_t blen, uint32_t wait_ms) {
    return frame_put(type, a, alen, b, blen, 0, pdMS_TO_TICKS(wait_ms));
}

bool link_raw(const void *data, size_t n) {
    if (xRingbufferSend(queue, data, n, pdMS_TO_TICKS(100)) != pdTRUE) {
        drops++;
        return false;
    }
    return true;
}

bool link_text(const char *text) { return link_raw(text, strlen(text)); }

void link_set_streaming(bool on) { streaming = on; }
IRAM_ATTR bool link_streaming(void) { return streaming; }

void link_flush(void) {
    size_t size;
    void *item;
    while ((item = xRingbufferReceive(queue, &size, 0)) != NULL) vRingbufferReturnItem(queue, item);
}

bool link_printf(const char *fmt, ...) {
    char buffer[512];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buffer, sizeof(buffer), fmt, ap);
    va_end(ap);
    return link_text(buffer);
}

int link_read(uint8_t *buf, size_t capacity, uint32_t timeout_ms) {
    return hw_read(buf, capacity, timeout_ms == UINT32_MAX ? portMAX_DELAY : pdMS_TO_TICKS(timeout_ms));
}
