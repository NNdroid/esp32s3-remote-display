#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <errno.h> // 用于 Socket 错误码处理
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "freertos/event_groups.h"
#include "esp_log.h"
#include "esp_wifi.h"
#include "esp_event.h"
#include "esp_system.h"
#include "nvs_flash.h"
#include "nvs.h"
#include "esp_netif.h"
#include "esp_mac.h"
#include "lwip/sockets.h"
#include <esp_http_server.h>
#include "esp_ota_ops.h"
#include "esp_https_ota.h"
#include <sys/param.h>
#include <time.h>
#include "esp_sntp.h"
#include "esp_task_wdt.h"

// 官方空闲钩子组件头文件
#include "esp_freertos_hooks.h"

// V6 官方蓝牙配网库
#include "network_provisioning/manager.h"
#include "network_provisioning/scheme_ble.h"

// 硬件核心驱动
#include "esp_lcd_panel_io.h"
#include "esp_lcd_panel_ops.h"
#include "esp_lcd_ili9341.h"
#include "driver/gpio.h"
#include "driver/spi_master.h"
#include "driver/ledc.h"

// 高性能解码器
#include "esp_jpeg_dec.h"

#define FRAME_BUF_SIZE 122880

// 屏幕分辨率巨集
#define LCD_WIDTH 320
#define LCD_HEIGHT 240
#define LCD_PIXELS (LCD_WIDTH * LCD_HEIGHT)

#define LCD_HOST SPI2_HOST
#define TFT_MISO 13
#define TFT_MOSI 11
#define TFT_SCK 12
#define TFT_CS 10
#define TFT_DC 46
#define TFT_RST -1
#define TFT_BL 45

#define DISPLAY_BUF_NUM 5
#define RX_BUF_NUM 12

// ==================== 全局状态与持久化配置 ====================
volatile int g_current_fps = 0;
volatile int8_t g_current_rssi = 0;

volatile int32_t g_current_brightness = 60;
volatile int8_t g_show_osd = 1;
volatile bool g_show_time_osd = true;
char g_admin_user[32] = "admin";
char g_admin_pwd[64] = "123456";
char g_device_ip[20] = "0.0.0.0";
char g_device_ip6[40] = "0000:0000:0000:0000:0000:0000:0000:0000";
char g_timezone[32] = "CST-8";

volatile int32_t g_udp_port = 8888;

volatile bool g_is_provisioning = false;

char g_prov_pop[9] = "";
uint8_t g_prov_uuid[16] = {0};

static EventGroupHandle_t s_wifi_event_group;
#define WIFI_CONNECTED_BIT BIT0
static int s_retry_num = 0;

// ==================== CPU 负载与系统监控非同步全局变量 ====================
volatile uint32_t g_core0_idle_cnt = 0;
volatile uint32_t g_core1_idle_cnt = 0;

uint32_t g_core0_max_idle = 0;
uint32_t g_core1_max_idle = 0;

volatile int g_core0_usage = 0;
volatile int g_core1_usage = 0;

// 讓 Core 0 代為跑沉重的 API，Core 1 解碼達到 0 延自由阻礙
volatile uint32_t g_async_free_heap_kb = 0;
volatile int8_t g_async_wifi_rssi = 0;
volatile char g_async_wifi_bssid[20] = {0};
volatile uint32_t g_rx_bytes_1s = 0;
volatile uint32_t g_current_rx_speed_kbps = 0;

// 统一的 FreeRTOS 空闲钩子函数
bool vApplicationIdleHookCore(void)
{
    BaseType_t core_id = xPortGetCoreID();
    if (core_id == 0)
    {
        g_core0_idle_cnt++;
    }
    else if (core_id == 1)
    {
        g_core1_idle_cnt++;
    }
    return true;
}

// ==================== 引入 HTML 静态档案 ====================
extern const uint8_t index_html_start[] asm("_binary_index_html_start");
extern const uint8_t index_html_end[] asm("_binary_index_html_end");
extern const uint8_t login_html_start[] asm("_binary_login_html_start");
extern const uint8_t login_html_end[] asm("_binary_login_html_end");

// ==================== 辅助工具函数声明与实现 ====================
static void url_decode_safe(char *dst, const char *src, size_t max_len)
{
    char a, b;
    size_t written = 0;
    while (*src && (written < max_len - 1))
    {
        if ((*src == '%') && ((a = src[1]) && (b = src[2])) && (isxdigit(a) && isxdigit(b)))
        {
            if (a >= 'a')
                a -= 'a' - 'A';
            else if (a >= 'A')
                a -= ('A' - 10);
            else
                a -= '0';
            if (b >= 'a')
                b -= 'a' - 'A';
            else if (b >= 'A')
                b -= ('A' - 10);
            else
                b -= '0';
            *dst++ = 16 * a + b;
            src += 3;
        }
        else if (*src == '+')
        {
            *dst++ = ' ';
            src++;
        }
        else
        {
            *dst++ = *src++;
        }
        written++;
    }
    *dst = '\0';
}

// OSD 字模点阵库
static const uint8_t font8x8[96][8] = {
    {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, {0x18, 0x3C, 0x3C, 0x18, 0x18, 0x00, 0x18, 0x00}, {0x6C, 0x6C, 0x6C, 0x00, 0x00, 0x00, 0x00, 0x00}, {0x6C, 0x6C, 0xFE, 0x6C, 0xFE, 0x6C, 0x6C, 0x00}, {0x18, 0x7E, 0x60, 0x7C, 0x06, 0x7E, 0x18, 0x00}, {0x00, 0x63, 0x66, 0x0C, 0x18, 0x33, 0x63, 0x00}, {0x1C, 0x36, 0x1C, 0x6E, 0x7B, 0x36, 0x71, 0x00}, {0x18, 0x18, 0x18, 0x00, 0x00, 0x00, 0x00, 0x00}, {0x0C, 0x18, 0x30, 0x30, 0x30, 0x18, 0x0C, 0x00}, {0x30, 0x18, 0x0C, 0x0C, 0x0C, 0x18, 0x30, 0x00}, {0x00, 0x12, 0x3F, 0x1E, 0x3F, 0x12, 0x00, 0x00}, {0x00, 0x18, 0x18, 0x7E, 0x18, 0x18, 0x00, 0x00}, {0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x18, 0x30}, {0x00, 0x00, 0x00, 0x7E, 0x00, 0x00, 0x00, 0x00}, {0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x18, 0x00}, {0x00, 0x06, 0x0C, 0x18, 0x30, 0x60, 0x00, 0x00}, {0x3C, 0x66, 0x6E, 0x76, 0x66, 0x66, 0x3C, 0x00}, {0x18, 0x38, 0x18, 0x18, 0x18, 0x18, 0x7E, 0x00}, {0x3C, 0x66, 0x06, 0x1C, 0x30, 0x66, 0x7E, 0x00}, {0x3C, 0x66, 0x06, 0x1C, 0x06, 0x66, 0x3C, 0x00}, {0x0C, 0x1C, 0x3C, 0x6C, 0x7E, 0x0C, 0x0C, 0x00}, {0x7E, 0x60, 0x7C, 0x06, 0x06, 0x66, 0x3C, 0x00}, {0x3C, 0x66, 0x60, 0x7C, 0x66, 0x66, 0x3C, 0x00}, {0x7E, 0x06, 0x0C, 0x18, 0x30, 0x30, 0x30, 0x00}, {0x3C, 0x66, 0x66, 0x3C, 0x66, 0x66, 0x3C, 0x00}, {0x3C, 0x66, 0x66, 0x3E, 0x06, 0x66, 0x3C, 0x00}, {0x00, 0x18, 0x18, 0x00, 0x00, 0x18, 0x18, 0x00}, {0x00, 0x18, 0x18, 0x00, 0x00, 0x18, 0x18, 0x30}, {0x06, 0x0C, 0x18, 0x30, 0x18, 0x0C, 0x06, 0x00}, {0x00, 0x00, 0x7E, 0x00, 0x7E, 0x00, 0x00, 0x00}, {0x60, 0x30, 0x18, 0x0C, 0x18, 0x30, 0x60, 0x00}, {0x3C, 0x66, 0x06, 0x1C, 0x18, 0x00, 0x18, 0x00}, {0x3C, 0x66, 0x6E, 0x6E, 0x60, 0x62, 0x3C, 0x00}, {0x18, 0x3C, 0x66, 0x66, 0x7E, 0x66, 0x66, 0x00}, {0x7C, 0x66, 0x66, 0x7C, 0x66, 0x66, 0x7C, 0x00}, {0x3C, 0x66, 0x60, 0x60, 0x60, 0x66, 0x3C, 0x00}, {0x78, 0x6C, 0x66, 0x66, 0x66, 0x6C, 0x78, 0x00}, {0x7E, 0x60, 0x60, 0x78, 0x60, 0x60, 0x7E, 0x00}, {0x7E, 0x60, 0x60, 0x78, 0x60, 0x60, 0x60, 0x00}, {0x3C, 0x66, 0x60, 0x6E, 0x66, 0x66, 0x3E, 0x00}, {0x66, 0x66, 0x66, 0x7E, 0x66, 0x66, 0x66, 0x00}, {0x3C, 0x18, 0x18, 0x18, 0x18, 0x18, 0x3C, 0x00}, {0x06, 0x06, 0x06, 0x06, 0x06, 0x66, 0x3C, 0x00}, {0x66, 0x6C, 0x78, 0x70, 0x78, 0x6C, 0x66, 0x00}, {0x60, 0x60, 0x60, 0x60, 0x60, 0x60, 0x7E, 0x00}, {0x63, 0x77, 0x7F, 0x6B, 0x63, 0x63, 0x63, 0x00}, {0x66, 0x76, 0x7E, 0x7E, 0x6E, 0x66, 0x66, 0x00}, {0x3C, 0x66, 0x66, 0x66, 0x66, 0x66, 0x3C, 0x00}, {0x7C, 0x66, 0x66, 0x7C, 0x60, 0x60, 0x60, 0x00}, {0x3C, 0x66, 0x66, 0x66, 0x66, 0x6E, 0x3E, 0x00}, {0x7C, 0x66, 0x66, 0x7C, 0x78, 0x6C, 0x66, 0x00}, {0x3C, 0x66, 0x60, 0x3C, 0x06, 0x66, 0x3C, 0x00}, {0x7E, 0x18, 0x18, 0x18, 0x18, 0x18, 0x18, 0x00}, {0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x3C, 0x00}, {0x66, 0x66, 0x66, 0x66, 0x66, 0x3C, 0x18, 0x00}, {0x63, 0x63, 0x6B, 0x7F, 0x77, 0x63, 0x63, 0x00}, {0x66, 0x66, 0x3C, 0x18, 0x3C, 0x66, 0x66, 0x00}, {0x66, 0x66, 0x66, 0x3C, 0x18, 0x18, 0x18, 0x00}, {0x7E, 0x06, 0x0C, 0x18, 0x30, 0x60, 0x7E, 0x00}, {0x3C, 0x30, 0x30, 0x30, 0x30, 0x30, 0x3C, 0x00}, {0x00, 0x60, 0x30, 0x18, 0x0C, 0x06, 0x00, 0x00}, {0x3C, 0x0C, 0x0C, 0x0C, 0x0C, 0x0C, 0x3C, 0x00}, {0x18, 0x3C, 0x66, 0x00, 0x00, 0x00, 0x00, 0x00}, {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF}, {0x18, 0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, {0x00, 0x00, 0x3C, 0x06, 0x3E, 0x66, 0x3E, 0x00}, {0x60, 0x60, 0x7C, 0x66, 0x66, 0x66, 0x7C, 0x00}, {0x00, 0x00, 0x3C, 0x60, 0x60, 0x60, 0x3C, 0x00}, {0x06, 0x06, 0x3E, 0x66, 0x66, 0x66, 0x3E, 0x00}, {0x00, 0x00, 0x3C, 0x66, 0x7E, 0x60, 0x3C, 0x00}, {0x1C, 0x30, 0x7C, 0x30, 0x30, 0x30, 0x30, 0x00}, {0x00, 0x00, 0x3E, 0x66, 0x66, 0x3E, 0x06, 0x3C}, {0x60, 0x60, 0x7C, 0x66, 0x66, 0x66, 0x66, 0x00}, {0x18, 0x00, 0x38, 0x18, 0x18, 0x18, 0x3C, 0x00}, {0x06, 0x00, 0x06, 0x06, 0x06, 0x66, 0x3C, 0x00}, {0x60, 0x60, 0x66, 0x6C, 0x78, 0x6C, 0x66, 0x00}, {0x38, 0x18, 0x18, 0x18, 0x18, 0x18, 0x3C, 0x00}, {0x00, 0x00, 0x6C, 0x7E, 0x66, 0x66, 0x66, 0x00}, {0x00, 0x00, 0x7C, 0x66, 0x66, 0x66, 0x66, 0x00}, {0x00, 0x00, 0x3C, 0x66, 0x66, 0x66, 0x3C, 0x00}, {0x00, 0x00, 0x7C, 0x66, 0x66, 0x7C, 0x60, 0x60}, {0x00, 0x00, 0x3E, 0x66, 0x66, 0x3E, 0x06, 0x06}, {0x00, 0x00, 0x7C, 0x66, 0x60, 0x60, 0x60, 0x00}, {0x00, 0x00, 0x3C, 0x60, 0x3C, 0x06, 0x3C, 0x00}, {0x30, 0x30, 0x7C, 0x30, 0x30, 0x30, 0x1C, 0x00}, {0x00, 0x00, 0x66, 0x66, 0x66, 0x66, 0x3E, 0x00}, {0x00, 0x00, 0x66, 0x66, 0x66, 0x3C, 0x18, 0x00}, {0x00, 0x00, 0x63, 0x6B, 0x7F, 0x77, 0x63, 0x00}, {0x00, 0x00, 0x66, 0x3C, 0x18, 0x3C, 0x66, 0x00}, {0x00, 0x00, 0x66, 0x66, 0x66, 0x3E, 0x06, 0x3C}, {0x00, 0x00, 0x7E, 0x0C, 0x18, 0x30, 0x7E, 0x00}, {0x0E, 0x18, 0x18, 0x70, 0x18, 0x18, 0x0E, 0x00}, {0x18, 0x18, 0x18, 0x00, 0x18, 0x18, 0x18, 0x00}, {0x70, 0x18, 0x18, 0x0E, 0x18, 0x18, 0x70, 0x00}, {0x3A, 0x6E, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}};

// 只重寫帶有點陣字模的像素點，完美保留 PSRAM 原圖背景背景
void draw_string_8x8_transparent_fast(uint16_t *frame_buffer, int x, int y, const char *str, uint16_t color)
{
    int cursor_x = x;
    while (*str)
    {
        char c = *str++;
        if (c < 32 || c > 126)
            continue;
        int font_idx = c - 32;

        if (cursor_x + 8 > LCD_WIDTH)
            break;

        uint16_t *char_base_ptr = frame_buffer + (y * LCD_WIDTH) + cursor_x;

        for (int i = 0; i < 8; i++)
        {
            if (y + i < 0 || y + i >= LCD_HEIGHT)
                continue;
            uint8_t line_data = font8x8[font_idx][i];
            uint16_t *pixel_ptr = char_base_ptr + (i * LCD_WIDTH);

            if (line_data & 0x80)
                pixel_ptr[0] = color;
            if (line_data & 0x40)
                pixel_ptr[1] = color;
            if (line_data & 0x20)
                pixel_ptr[2] = color;
            if (line_data & 0x10)
                pixel_ptr[3] = color;
            if (line_data & 0x08)
                pixel_ptr[4] = color;
            if (line_data & 0x04)
                pixel_ptr[5] = color;
            if (line_data & 0x02)
                pixel_ptr[6] = color;
            if (line_data & 0x01)
                pixel_ptr[7] = color;
        }
        cursor_x += 8;
    }
}

static void draw_provisioning_screen(esp_lcd_panel_handle_t panel, uint16_t *full_frame)
{
    for (int i = 0; i < LCD_PIXELS; i++)
        full_frame[i] = 0x0000;
    char pop_str[32];
    snprintf(pop_str, sizeof(pop_str), "PoP Key : %s", g_prov_pop);
    char uuid_val_str[64];
    snprintf(uuid_val_str, sizeof(uuid_val_str), "%02X%02X%02X%02X-%02X%02X-%02X%02X-%02X%02X-%02X%02X%02X%02X%02X%02X",
             g_prov_uuid[0], g_prov_uuid[1], g_prov_uuid[2], g_prov_uuid[3],
             g_prov_uuid[4], g_prov_uuid[5], g_prov_uuid[6], g_prov_uuid[7],
             g_prov_uuid[8], g_prov_uuid[9], g_prov_uuid[10], g_prov_uuid[11],
             g_prov_uuid[12], g_prov_uuid[13], g_prov_uuid[14], g_prov_uuid[15]);

    draw_string_8x8_transparent_fast(full_frame, 20, 60, "Waiting for BLE Provisioning...", 0xFFFF);
    draw_string_8x8_transparent_fast(full_frame, 20, 90, "Device Name : PROV_LCD", 0xE007);
    draw_string_8x8_transparent_fast(full_frame, 20, 110, pop_str, 0xE007);
    draw_string_8x8_transparent_fast(full_frame, 20, 140, "Service UUID:", 0xE007);
    draw_string_8x8_transparent_fast(full_frame, 20, 160, uuid_val_str, 0xE007);
    esp_lcd_panel_draw_bitmap(panel, 0, 0, LCD_WIDTH, LCD_HEIGHT, full_frame);
}

static void draw_no_signal_screen(esp_lcd_panel_handle_t panel, uint16_t *full_frame)
{
    for (int i = 0; i < LCD_PIXELS; i++)
        full_frame[i] = 0x1F00;
    const char *msg = "No Signal";
    int scale = 3;
    int text_w = strlen(msg) * 8 * scale;
    int text_h = 8 * scale;
    int start_x = (LCD_WIDTH - text_w) / 2;
    int start_y = (LCD_HEIGHT - text_h) / 2;
    int cur_x = start_x;
    for (int i = 0; msg[i] != '\0'; i++)
    {
        char c = msg[i];
        if (c >= 32 && c <= 126)
        {
            int font_idx = c - 32;
            for (int row = 0; row < 8; row++)
            {
                uint8_t pixel_row = font8x8[font_idx][row];
                for (int col = 0; col < 8; col++)
                {
                    if (pixel_row & (1 << (7 - col)))
                    {
                        for (int dy = 0; dy < scale; dy++)
                        {
                            for (int dx = 0; dx < scale; dx++)
                            {
                                int px = cur_x + col * scale + dx;
                                int py = start_y + row * scale + dy;
                                if (px < LCD_WIDTH && py < LCD_HEIGHT)
                                    full_frame[py * LCD_WIDTH + px] = 0xFFFF;
                            }
                        }
                    }
                }
            }
        }
        cur_x += 8 * scale;
    }
    esp_lcd_panel_draw_bitmap(panel, 0, 0, LCD_WIDTH, LCD_HEIGHT, full_frame);
}

typedef struct
{
    uint8_t *buf_ptr;
    uint32_t len;
} rx_frame_t;

static QueueHandle_t jpeg_queue = NULL;
static QueueHandle_t free_queue = NULL; // 空闲缓冲区队列
static esp_lcd_panel_handle_t panel_handle = NULL;
static esp_lcd_panel_io_handle_t io_handle = NULL;

static uint8_t *rx_frame_buf[RX_BUF_NUM] = {NULL};
static uint8_t *display_buf[DISPLAY_BUF_NUM] = {NULL};
static uint8_t current_buffer_idx = 0;

// ==================== 系统与配网状态机 ====================
static void sys_event_handler(void *arg, esp_event_base_t event_base, int32_t event_id, void *event_data)
{
    if (event_base == NETWORK_PROV_EVENT)
    {
        switch (event_id)
        {
        case NETWORK_PROV_START:
            g_is_provisioning = true;
            break;
        case NETWORK_PROV_WIFI_CRED_FAIL:
            network_prov_mgr_reset_wifi_sm_state_on_failure();
            break;
        case NETWORK_PROV_END:
            g_is_provisioning = false;
            network_prov_mgr_deinit();
            break;
        default:
            break;
        }
    }
    else if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START)
        esp_wifi_connect();
    else if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_SCAN_DONE)
    {
        uint16_t ap_count = 0;
        esp_wifi_scan_get_ap_num(&ap_count);
        if (ap_count > 20)
            ap_count = 20; // Limit dynamically if too many
        if (ap_count > 0)
        {
            wifi_ap_record_t *ap_list = (wifi_ap_record_t *)malloc(sizeof(wifi_ap_record_t) * ap_count);
            if (ap_list && esp_wifi_scan_get_ap_records(&ap_count, ap_list) == ESP_OK)
            {
                wifi_config_t wifi_cfg;
                if (esp_wifi_get_config(WIFI_IF_STA, &wifi_cfg) == ESP_OK)
                {
                    wifi_ap_record_t current_ap;
                    if (esp_wifi_sta_get_ap_info(&current_ap) == ESP_OK)
                    {
                        int best_rssi = -127;
                        int best_idx = -1;
                        for (int i = 0; i < ap_count; i++)
                        {
                            if (strcmp((char *)ap_list[i].ssid, (char *)wifi_cfg.sta.ssid) == 0)
                            {
                                if (ap_list[i].rssi > best_rssi)
                                {
                                    best_rssi = ap_list[i].rssi;
                                    best_idx = i;
                                }
                            }
                        }
                        if (best_idx >= 0 && best_rssi > current_ap.rssi + 5)
                        {
                            if (memcmp(ap_list[best_idx].bssid, current_ap.bssid, 6) != 0)
                            {
                                ESP_LOGI("WIFI", "Roaming to stronger AP: %02X:%02X:%02X:%02X:%02X:%02X (RSSI: %d -> %d)",
                                         ap_list[best_idx].bssid[0], ap_list[best_idx].bssid[1], ap_list[best_idx].bssid[2],
                                         ap_list[best_idx].bssid[3], ap_list[best_idx].bssid[4], ap_list[best_idx].bssid[5],
                                         current_ap.rssi, best_rssi);
                                memcpy(wifi_cfg.sta.bssid, ap_list[best_idx].bssid, 6);
                                wifi_cfg.sta.bssid_set = true;
                                esp_wifi_set_config(WIFI_IF_STA, &wifi_cfg);
                                esp_wifi_disconnect();
                            }
                        }
                    }
                }
            }
            if (ap_list)
                free(ap_list);
            else
                esp_wifi_scan_get_ap_records(&ap_count, NULL);
        }
        else
        {
            esp_wifi_scan_get_ap_records(&ap_count, NULL);
        }
    }
    else if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_DISCONNECTED)
    {
        xEventGroupClearBits(s_wifi_event_group, WIFI_CONNECTED_BIT);
        if (g_is_provisioning)
            esp_wifi_connect();
        else
        {
            if (s_retry_num < 300)
            {
                s_retry_num++;
                esp_wifi_connect();
            }
            else
            {
                // Do not reset provisioning on router reboot, just restart device to retry
                esp_restart();
            }
        }
    }
    else if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_CONNECTED)
    {
        ESP_LOGI("WIFI", "Connected to AP, triggering IPv6...");
        // 拿到默认 STA 句柄并启动 IPv6 本地链路生成
        esp_netif_t *netif = esp_netif_get_handle_from_ifkey("WIFI_STA_DEF");
        if (netif) {
            esp_netif_create_ip6_linklocal(netif);
        }
    }
    else if (event_base == IP_EVENT && event_id == IP_EVENT_STA_GOT_IP)
    {
        wifi_config_t wifi_cfg;
        if (esp_wifi_get_config(WIFI_IF_STA, &wifi_cfg) == ESP_OK)
        {
            if (wifi_cfg.sta.bssid_set)
            {
                wifi_cfg.sta.bssid_set = false;
                esp_wifi_set_config(WIFI_IF_STA, &wifi_cfg);
                ESP_LOGI("WIFI", "Cleared BSSID lock");
            }
        }
        ip_event_got_ip_t *event = (ip_event_got_ip_t *)event_data;
        sprintf(g_device_ip, IPSTR, IP2STR(&event->ip_info.ip));
        s_retry_num = 0;
        xEventGroupSetBits(s_wifi_event_group, WIFI_CONNECTED_BIT);
    }
    else if (event_base == IP_EVENT && event_id == IP_EVENT_GOT_IP6)
    {
        ip_event_got_ip6_t *event = (ip_event_got_ip6_t *)event_data;
        sprintf(g_device_ip6, IPV6STR, IPV62STR(event->ip6_info.ip));
        ESP_LOGI("WIFI", "Got IPv6 Address: %s", g_device_ip6);
    }
}

// ==================== NVS 持久化函数 ====================
static void load_settings_from_nvs()
{
    nvs_handle_t my_handle;
    if (nvs_open("storage", NVS_READWRITE, &my_handle) == ESP_OK)
    {
        int32_t saved_br = 60;
        if (nvs_get_i32(my_handle, "brightness", &saved_br) == ESP_OK)
            g_current_brightness = saved_br;

        int8_t saved_osd = 1;
        if (nvs_get_i8(my_handle, "show_osd", &saved_osd) == ESP_OK)
            g_show_osd = saved_osd;

        int8_t saved_time_osd = 1;
        if (nvs_get_i8(my_handle, "show_time", &saved_time_osd) == ESP_OK)
            g_show_time_osd = saved_time_osd;

        size_t len_tz = sizeof(g_timezone);
        if (nvs_get_str(my_handle, "tz_str", g_timezone, &len_tz) != ESP_OK)
            strcpy(g_timezone, "CST-8");

        size_t len = sizeof(g_admin_user);
        if (nvs_get_str(my_handle, "admin_user", g_admin_user, &len) != ESP_OK)
            strcpy(g_admin_user, "admin");

        len = sizeof(g_admin_pwd);
        if (nvs_get_str(my_handle, "admin_pwd", g_admin_pwd, &len) != ESP_OK)
            strcpy(g_admin_pwd, "123456");

        int32_t saved_port = 8888;
        if (nvs_get_i32(my_handle, "udp_port", &saved_port) == ESP_OK)
            g_udp_port = saved_port;

        size_t len_pop = sizeof(g_prov_pop);
        if (nvs_get_str(my_handle, "prov_pop", g_prov_pop, &len_pop) != ESP_OK)
        {
            snprintf(g_prov_pop, sizeof(g_prov_pop), "%08lu", esp_random() % 100000000);
            nvs_set_str(my_handle, "prov_pop", g_prov_pop);
        }

        size_t len_uuid = sizeof(g_prov_uuid);
        if (nvs_get_blob(my_handle, "prov_uuid", g_prov_uuid, &len_uuid) != ESP_OK)
        {
            esp_fill_random(g_prov_uuid, sizeof(g_prov_uuid));
            g_prov_uuid[6] = (g_prov_uuid[6] & 0x0F) | 0x40;
            g_prov_uuid[8] = (g_prov_uuid[8] & 0x3F) | 0x80;
            nvs_set_blob(my_handle, "prov_uuid", g_prov_uuid, sizeof(g_prov_uuid));
        }

        nvs_commit(my_handle);
        nvs_close(my_handle);
    }
}

static void save_int_to_nvs(const char *key, int32_t val)
{
    nvs_handle_t my_handle;
    if (nvs_open("storage", NVS_READWRITE, &my_handle) == ESP_OK)
    {
        nvs_set_i32(my_handle, key, val);
        nvs_commit(my_handle);
        nvs_close(my_handle);
    }
}
static void save_i8_to_nvs(const char *key, int8_t val)
{
    nvs_handle_t my_handle;
    if (nvs_open("storage", NVS_READWRITE, &my_handle) == ESP_OK)
    {
        nvs_set_i8(my_handle, key, val);
        nvs_commit(my_handle);
        nvs_close(my_handle);
    }
}
static void save_str_to_nvs(const char *key, const char *val)
{
    nvs_handle_t my_handle;
    if (nvs_open("storage", NVS_READWRITE, &my_handle) == ESP_OK)
    {
        nvs_set_str(my_handle, key, val);
        nvs_commit(my_handle);
        nvs_close(my_handle);
    }
}

// ==================== 硬件初始化 ====================
static void lcd_hardware_init(void)
{
    spi_bus_config_t buscfg = {
        .sclk_io_num = TFT_SCK,
        .mosi_io_num = TFT_MOSI,
        .miso_io_num = TFT_MISO,
        .quadwp_io_num = -1,
        .quadhd_io_num = -1,
        .max_transfer_sz = (LCD_PIXELS * 2) + 8};
    ESP_ERROR_CHECK(spi_bus_initialize(LCD_HOST, &buscfg, SPI_DMA_CH_AUTO));
    gpio_set_drive_capability(TFT_SCK, GPIO_DRIVE_CAP_3);
    gpio_set_drive_capability(TFT_MOSI, GPIO_DRIVE_CAP_3);
    gpio_set_drive_capability(TFT_CS, GPIO_DRIVE_CAP_3);
    gpio_set_drive_capability(TFT_DC, GPIO_DRIVE_CAP_3);

    esp_lcd_panel_io_spi_config_t io_config = {.dc_gpio_num = TFT_DC, .cs_gpio_num = TFT_CS, .pclk_hz = 40 * 1000 * 1000, .lcd_cmd_bits = 8, .lcd_param_bits = 8, .spi_mode = 0, .trans_queue_depth = 20};
    ESP_ERROR_CHECK(esp_lcd_new_panel_io_spi((esp_lcd_spi_bus_handle_t)LCD_HOST, &io_config, &io_handle));
    esp_lcd_panel_dev_config_t panel_config = {.reset_gpio_num = TFT_RST, .bits_per_pixel = 16};
    ESP_ERROR_CHECK(esp_lcd_new_panel_ili9341(io_handle, &panel_config, &panel_handle));
    ESP_ERROR_CHECK(esp_lcd_panel_reset(panel_handle));
    vTaskDelay(pdMS_TO_TICKS(120));
    ESP_ERROR_CHECK(esp_lcd_panel_init(panel_handle));
    vTaskDelay(pdMS_TO_TICKS(50));
    ESP_ERROR_CHECK(esp_lcd_panel_swap_xy(panel_handle, true));
    uint8_t madctl_val = 0x28;
    esp_lcd_panel_io_tx_param(io_handle, 0x36, &madctl_val, 1);
    ESP_ERROR_CHECK(esp_lcd_panel_invert_color(panel_handle, true));
    ESP_ERROR_CHECK(esp_lcd_panel_disp_on_off(panel_handle, true));

    load_settings_from_nvs();
    uint32_t init_duty = (g_current_brightness * 255) / 100;
    ledc_timer_config_t ledc_timer = {.speed_mode = LEDC_LOW_SPEED_MODE, .duty_resolution = LEDC_TIMER_8_BIT, .timer_num = LEDC_TIMER_0, .freq_hz = 5000, .clk_cfg = LEDC_AUTO_CLK};
    ledc_timer_config(&ledc_timer);
    ledc_channel_config_t ledc_channel = {.speed_mode = LEDC_LOW_SPEED_MODE, .channel = LEDC_CHANNEL_0, .timer_sel = LEDC_TIMER_0, .intr_type = LEDC_INTR_DISABLE, .gpio_num = TFT_BL, .duty = init_duty, .hpoint = 0};
    ledc_channel_config(&ledc_channel);
}

void jpeg_decode_display_task(void *pvParameters)
{
    esp_task_wdt_add(NULL);
    int fps_counter = 0;
    int current_fps = 0;
    int8_t current_rssi = 0;
    int screen_state = 0;
    TickType_t last_frame_time = xTaskGetTickCount(), last_fps_time = xTaskGetTickCount();
    static int last_screen_state = -1;

    // 16行 MCU 行解碼流水線。一步到位直接硬转换输出 RGB565_BE
    jpeg_dec_config_t config = {
        .output_type = JPEG_PIXEL_FORMAT_RGB565_BE,
        .scale = {.width = 0, .height = 0},
        .clipper = {.width = 0, .height = 0},
        .rotate = JPEG_ROTATE_0D,
        .block_enable = true};

    while (1)
    {
        esp_task_wdt_reset();
        rx_frame_t current_frame;
        if (xQueueReceive(jpeg_queue, &current_frame, pdMS_TO_TICKS(10)) == pdTRUE)
        {
            if (current_frame.len < 1024)
            {
                xQueueSend(free_queue, &current_frame, 0);
                continue;
            }
            last_frame_time = xTaskGetTickCount();
            screen_state = 1;
            jpeg_dec_handle_t jpeg_dec = NULL;
            if (jpeg_dec_open(&config, &jpeg_dec) != JPEG_ERR_OK)
            {
                xQueueSend(free_queue, &current_frame, 0);
                continue;
            }

            jpeg_dec_io_t jpeg_io = {0};
            jpeg_dec_header_info_t out_info = {0};
            jpeg_io.inbuf = current_frame.buf_ptr;
            jpeg_io.inbuf_len = current_frame.len;

            if (jpeg_dec_parse_header(jpeg_dec, &jpeg_io, &out_info) == JPEG_ERR_OK)
            {
                if (out_info.width > LCD_WIDTH || out_info.height > LCD_HEIGHT)
                {
                    ESP_LOGE("JPEG", "Frame too large: %dx%d, dropping!", (int)out_info.width, (int)out_info.height);
                    jpeg_dec_close(jpeg_dec);
                    xQueueSend(free_queue, &current_frame, 0);
                    continue;
                }

                uint16_t *full_frame = (uint16_t *)display_buf[current_buffer_idx];

                // 16行 16行 地跑，完全规避 PSRAM 慢速，彻底消灭 Cache Miss！
                int cur_row = 0;
                bool decode_ok = true;
                while (cur_row < out_info.height)
                {
                    jpeg_io.outbuf = (uint8_t *)(full_frame + (cur_row * LCD_WIDTH));

                    int inbuf_consumed = jpeg_io.inbuf_len - jpeg_io.inbuf_remain;
                    jpeg_io.inbuf += inbuf_consumed;
                    jpeg_io.inbuf_len = jpeg_io.inbuf_remain;

                    jpeg_io.out_size = 0;

                    if (jpeg_dec_process(jpeg_dec, &jpeg_io) != JPEG_ERR_OK)
                    {
                        decode_ok = false;
                        break;
                    }

                    if (jpeg_io.out_size == 0 || out_info.width == 0)
                        break;

                    int rows_decoded = jpeg_io.out_size / (out_info.width * 2);
                    if (rows_decoded == 0)
                    {
                        decode_ok = false;
                        break;
                    }

                    cur_row += rows_decoded;
                }

                if (decode_ok)
                {
                    fps_counter++;
                    TickType_t now = xTaskGetTickCount();

                    static char cached_osd_str[32] = {0}, cached_time_str[32] = {0}, cached_heap_str[32] = {0}, cached_cpu_str[32] = {0}, cached_net_str[32] = {0}, cached_bssid_str[32] = {0};
                    static int cached_time_width = 0;

                    if (now - last_fps_time >= pdMS_TO_TICKS(1000))
                    {
                        current_fps = fps_counter;
                        fps_counter = 0;
                        last_fps_time = now;
                        g_current_fps = current_fps;

                        uint32_t max_idle_1s_c0 = g_core0_max_idle * 5;
                        uint32_t max_idle_1s_c1 = g_core1_max_idle * 5;

                        uint32_t idle_c0 = g_core0_idle_cnt;
                        g_core0_idle_cnt = 0;
                        uint32_t idle_c1 = g_core1_idle_cnt;
                        g_core1_idle_cnt = 0;

                        int cpu0 = 100 - (idle_c0 * 100 / max_idle_1s_c0);
                        int cpu1 = 100 - (idle_c1 * 100 / max_idle_1s_c1);

                        if (cpu0 < 0)
                        {
                            cpu0 = 0;
                        }
                        if (cpu0 > 100)
                        {
                            cpu0 = 100;
                        }
                        if (cpu1 < 0)
                        {
                            cpu1 = 0;
                        }
                        if (cpu1 > 100)
                        {
                            cpu1 = 100;
                        }
                        g_core0_usage = cpu0;
                        g_core1_usage = cpu1;

                        current_rssi = g_async_wifi_rssi;
                        g_current_rssi = current_rssi;

                        snprintf(cached_cpu_str, sizeof(cached_cpu_str), "C0:%d%% C1:%d%%", cpu0, cpu1);
                        snprintf(cached_osd_str, sizeof(cached_osd_str), "%ddB %dFPS", current_rssi, current_fps);
                        snprintf(cached_heap_str, sizeof(cached_heap_str), "Heap:%luKB", (unsigned long)g_async_free_heap_kb);
                        snprintf(cached_net_str, sizeof(cached_net_str), "DL:%luKB/s", (unsigned long)g_current_rx_speed_kbps);
                        strncpy(cached_bssid_str, (const char *)g_async_wifi_bssid, sizeof(cached_bssid_str) - 1);

                        time_t now_time;
                        struct tm timeinfo;
                        time(&now_time);
                        localtime_r(&now_time, &timeinfo);
                        strftime(cached_time_str, sizeof(cached_time_str), "%Y-%m-%d %H:%M:%S", &timeinfo);
                        cached_time_width = strlen(cached_time_str) * 8;
                        ESP_LOGI("SYS", "Free: %lu, MaxBlock: %lu, StackWaterMark: %d",
                                 esp_get_free_heap_size(),
                                 heap_caps_get_largest_free_block(MALLOC_CAP_SPIRAM),
                                 uxTaskGetStackHighWaterMark(NULL));
                    }

                    if (g_show_osd)
                    {
                        if (cached_bssid_str[0] != '\0')
                        {
                            draw_string_8x8_transparent_fast(full_frame, 2, LCD_HEIGHT - 50, cached_bssid_str, 0xFFFF);
                        }
                        if (cached_net_str[0] != '\0')
                        {
                            draw_string_8x8_transparent_fast(full_frame, 2, LCD_HEIGHT - 40, cached_net_str, 0xFFFF);
                        }
                        if (cached_osd_str[0] != '\0')
                        {
                            draw_string_8x8_transparent_fast(full_frame, 2, LCD_HEIGHT - 30, cached_osd_str, 0xE007);
                        }
                        if (cached_cpu_str[0] != '\0')
                        {
                            draw_string_8x8_transparent_fast(full_frame, 2, LCD_HEIGHT - 20, cached_cpu_str, 0x07E0);
                        }
                        if (cached_heap_str[0] != '\0')
                        {
                            draw_string_8x8_transparent_fast(full_frame, 2, LCD_HEIGHT - 10, cached_heap_str, 0x07FF);
                        }
                    }

                    if (g_show_time_osd && cached_time_str[0] != '\0')
                    {
                        int x_pos = LCD_WIDTH - cached_time_width - 2;
                        draw_string_8x8_transparent_fast(full_frame, x_pos, 2, cached_time_str, 0xFFFF);
                    }

                    // 将拼装好的成品一次性抛给底层 DMA 推送到 ILI9341
                    esp_lcd_panel_draw_bitmap(panel_handle, 0, 0, LCD_WIDTH, LCD_HEIGHT, full_frame);

                    current_buffer_idx = (current_buffer_idx + 1) % DISPLAY_BUF_NUM;
                    last_screen_state = 1;
                }
            }
            jpeg_dec_close(jpeg_dec);
            xQueueSend(free_queue, &current_frame, 0);
            vTaskDelay(1);
        }
        else
        {
            if (screen_state == 1 && xTaskGetTickCount() - last_frame_time > pdMS_TO_TICKS(5000))
                screen_state = 0;
            vTaskDelay(pdMS_TO_TICKS(200));
        }

        if (screen_state == 0)
        {
            if (last_screen_state != 0)
            {
                if (g_is_provisioning)
                    draw_provisioning_screen(panel_handle, (uint16_t *)display_buf[current_buffer_idx]);
                else
                    draw_no_signal_screen(panel_handle, (uint16_t *)display_buf[current_buffer_idx]);
                current_buffer_idx = (current_buffer_idx + 1) % DISPLAY_BUF_NUM;
            }
            last_screen_state = 0;
        }
    }
}

void udp_receiver_task(void *pvParameters) {
    int rcv_buf_size = 65536;
    struct timeval timeout = { .tv_sec = 1, .tv_usec = 0 };
    TickType_t last_sync_time = xTaskGetTickCount();
    TickType_t last_scan_time = 0;

    uint8_t read_buf[2048]; // 用於接收 UDP 封包 (通常 FFmpeg 每個 UDP 包含 7 個 TS 包，即 1316 Bytes)

    while (1) {
        xEventGroupWaitBits(s_wifi_event_group, WIFI_CONNECTED_BIT, pdFALSE, pdTRUE, portMAX_DELAY);
        // 改為 AF_INET6
        int sock = socket(AF_INET6, SOCK_DGRAM, IPPROTO_IP);
        if (sock < 0) { vTaskDelay(pdMS_TO_TICKS(1000)); continue; }
        
        // 關閉 IPV6_V6ONLY，開啟 Dual Stack (雙棧) 模式，使其能同時接收 IPv4 與 IPv6 推流
        int v6only = 0;
        setsockopt(sock, IPPROTO_IPV6, IPV6_V6ONLY, &v6only, sizeof(v6only));

        setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &rcv_buf_size, sizeof(rcv_buf_size));
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

        // 使用 sockaddr_in6 結構體
        struct sockaddr_in6 dest_addr = { 0 };
        dest_addr.sin6_family = AF_INET6;
        dest_addr.sin6_port = htons(g_udp_port);
        dest_addr.sin6_addr = in6addr_any; // in6addr_any 同時包含 0.0.0.0 與 ::

        if (bind(sock, (struct sockaddr *)&dest_addr, sizeof(dest_addr)) < 0) { 
            close(sock); 
            vTaskDelay(pdMS_TO_TICKS(1000)); 
            continue; 
        }

        uint32_t current_frame_len = 0;
        rx_frame_t current_rx_frame;
        bool has_active_buffer = false;
        
        // TS 解析狀態變數
        uint16_t video_pid = 0x1FFF; // 初始無效 PID
        int8_t last_cc = -1;
        bool frame_corrupted = true; // 預設丟棄，直到看見新幀的起點

        while (1) {
            if ((xEventGroupGetBits(s_wifi_event_group) & WIFI_CONNECTED_BIT) == 0) break;

            // --- 維持你原本的系統狀態監控邏輯 ---
            TickType_t now_tick = xTaskGetTickCount();
            if (now_tick - last_sync_time >= pdMS_TO_TICKS(1000)) {
                g_async_free_heap_kb = esp_get_free_heap_size() / 1024;
                wifi_ap_record_t ap_info;
                if (esp_wifi_sta_get_ap_info(&ap_info) == ESP_OK) {
                    g_async_wifi_rssi = ap_info.rssi;
                    snprintf((char *)g_async_wifi_bssid, sizeof(g_async_wifi_bssid), "%02X:%02X:%02X:%02X:%02X:%02X",
                             ap_info.bssid[0], ap_info.bssid[1], ap_info.bssid[2], ap_info.bssid[3], ap_info.bssid[4], ap_info.bssid[5]);
                    
                    if (ap_info.rssi < -60 && now_tick - last_scan_time >= pdMS_TO_TICKS(10000)) {
                        wifi_scan_config_t scan_config = {0}; scan_config.ssid = ap_info.ssid;
                        if (esp_wifi_scan_start(&scan_config, false) == ESP_OK) last_scan_time = now_tick;
                    }
                } else { g_async_wifi_rssi = 0; g_async_wifi_bssid[0] = '\0'; }
                g_current_rx_speed_kbps = g_rx_bytes_1s / 1024; g_rx_bytes_1s = 0; last_sync_time = now_tick;
            }

            // 接收 UDP 數據
            int len = recvfrom(sock, read_buf, sizeof(read_buf), 0, NULL, NULL);
            
            if (len > 0) {
                g_rx_bytes_1s += len;

                // 遍歷 UDP 封包內的每一個 188 Bytes TS 封包
                for (int offset = 0; offset <= len - 188; offset += 188) {
                    uint8_t *ts = &read_buf[offset];
                    
                    // TS 同步字節檢查
                    if (ts[0] != 0x47) continue; 
                    
                    uint16_t pid = ((ts[1] & 0x1F) << 8) | ts[2];
                    uint8_t pusi = (ts[1] & 0x40) >> 6;      // Payload Unit Start Indicator (新幀開始標誌)
                    uint8_t afc = (ts[3] & 0x30) >> 4;       // Adaptation Field Control
                    uint8_t cc = ts[3] & 0x0F;               // Continuity Counter (0-15 循環)
                    
                    if (afc == 0 || afc == 2) continue; // 沒有 Payload 的包直接跳過
                    
                    int payload_offset = 4;
                    if (afc == 3) {
                        payload_offset += 1 + ts[4]; // 跳過 Adaptation Field
                    }
                    if (payload_offset >= 188) continue;
                    
                    uint8_t *payload = &ts[payload_offset];
                    int payload_len = 188 - payload_offset;
                    
                    // 如果是新的一幀 (PES Packet Start)
                    if (pusi) {
                        // 尋找影片 PES Header (0x00 0x00 0x01 0xE0)
                        if (payload_len >= 6 && payload[0] == 0x00 && payload[1] == 0x00 && payload[2] == 0x01 && payload[3] == 0xE0) {
                            video_pid = pid; // 自動鎖定影片軌的 PID
                            
                            // 跳過 PES Header (通常是 9 Bytes + 附加長度)
                            int pes_header_len = 9 + payload[8];
                            if (payload_len > pes_header_len) {
                                payload += pes_header_len;
                                payload_len -= pes_header_len;
                                
                                // 準備新的緩衝區
                                if (!has_active_buffer) {
                                    if (xQueueReceive(free_queue, &current_rx_frame, 0) == pdTRUE) {
                                        has_active_buffer = true;
                                    }
                                }
                                
                                // 如果成功拿到緩衝區，才開始接收新幀
                                if (has_active_buffer) {
                                    current_frame_len = 0;
                                    frame_corrupted = false;
                                    last_cc = cc;
                                } else {
                                    frame_corrupted = true;
                                }
                            } else {
                                continue;
                            }
                        }
                    }
                    
                    // 處理鎖定的視訊流資料
                    if (pid == video_pid && has_active_buffer) {
                        // 檢查丟包 (連續性計數器)
                        if (!pusi) {
                            int expected_cc = (last_cc + 1) & 0x0F;
                            if (cc != expected_cc) {
                                ESP_LOGW("TS", "Packet drop detected! Expected CC %d, got %d. Dropping frame.", expected_cc, cc);
                                frame_corrupted = true; // 發生丟包，標記此幀為損壞，硬體安全了！
                            }
                            last_cc = cc;
                        }
                        
                        // 如果這幀完美無缺，就將數據拷貝到 PSRAM
                        if (!frame_corrupted) {
                            if (current_frame_len + payload_len <= FRAME_BUF_SIZE) {
                                memcpy(current_rx_frame.buf_ptr + current_frame_len, payload, payload_len);
                                current_frame_len += payload_len;
                                
                                // 快速偵測 JPEG 結尾 (FF D9)，一旦出現立刻送入解碼器，降低延遲
                                if (current_frame_len >= 2 &&
                                    current_rx_frame.buf_ptr[current_frame_len-2] == 0xFF &&
                                    current_rx_frame.buf_ptr[current_frame_len-1] == 0xD9) {
                                    
                                    current_rx_frame.len = current_frame_len;
                                    if (xQueueSend(jpeg_queue, &current_rx_frame, 0) == pdTRUE) {
                                        has_active_buffer = false;
                                    }
                                    // 幀已經送出，等待下一個 PUSI 到來
                                    frame_corrupted = true; 
                                }
                            } else {
                                ESP_LOGW("TS", "Frame overflow > %d Bytes!", FRAME_BUF_SIZE);
                                frame_corrupted = true; // 超出緩衝區，放棄此幀
                            }
                        }
                    }
                }
            } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
                ESP_LOGE("UDP", "Socket error: %d", errno);
                vTaskDelay(pdMS_TO_TICKS(10));
            }
        }
        
        if (has_active_buffer) {
            xQueueSend(free_queue, &current_rx_frame, 0);
            has_active_buffer = false;
        }
        close(sock);
    }
}

// ==================== Web 路由处理器 ====================
static bool is_authenticated(httpd_req_t *req)
{
    char cookie[128];
    if (httpd_req_get_hdr_value_str(req, "Cookie", cookie, sizeof(cookie)) == ESP_OK)
    {
        if (strstr(cookie, "session=esp32_admin") != NULL)
            return true;
    }
    return false;
}

static esp_err_t login_get_handler(httpd_req_t *req)
{
    const size_t html_size = (login_html_end - login_html_start);
    httpd_resp_set_type(req, "text/html");
    httpd_resp_send(req, (const char *)login_html_start, html_size);
    return ESP_OK;
}

static esp_err_t do_login_handler(httpd_req_t *req)
{
    char buf[150];
    if (httpd_req_get_url_query_str(req, buf, sizeof(buf)) == ESP_OK)
    {
        char user[32], pwd[64];
        if (httpd_query_key_value(buf, "user", user, sizeof(user)) == ESP_OK && httpd_query_key_value(buf, "pwd", pwd, sizeof(pwd)) == ESP_OK)
        {
            if (strcmp(user, g_admin_user) == 0 && strcmp(pwd, g_admin_pwd) == 0)
            {
                httpd_resp_set_hdr(req, "Set-Cookie", "session=esp32_admin; Path=/; Max-Age=86400");
                httpd_resp_set_status(req, "302 Found");
                httpd_resp_set_hdr(req, "Location", "/");
                httpd_resp_send(req, NULL, 0);
                return ESP_OK;
            }
        }
    }
    httpd_resp_set_status(req, "302 Found");
    httpd_resp_set_hdr(req, "Location", "/login?err=1");
    return ESP_OK;
}

static esp_err_t do_restart_handler(httpd_req_t *req)
{
    if (!is_authenticated(req))
    {
        httpd_resp_set_status(req, "401 Unauthorized");
        httpd_resp_send(req, "Unauthorized", HTTPD_RESP_USE_STRLEN);
        return ESP_FAIL;
    }
    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, "{\"success\":true,\"message\":\"设备正在重启...\"}", HTTPD_RESP_USE_STRLEN);
    vTaskDelay(pdMS_TO_TICKS(1000));
    esp_restart();
    return ESP_OK;
}

static esp_err_t index_get_handler(httpd_req_t *req)
{
    if (!is_authenticated(req))
    {
        httpd_resp_set_status(req, "302 Found");
        httpd_resp_set_hdr(req, "Location", "/login");
        httpd_resp_send(req, NULL, 0);
        return ESP_OK;
    }
    const size_t html_size = (index_html_end - index_html_start);
    httpd_resp_set_type(req, "text/html");
    httpd_resp_send(req, (const char *)index_html_start, html_size);
    return ESP_OK;
}

static esp_err_t api_ota_handler(httpd_req_t *req)
{
    if (!is_authenticated(req))
    {
        httpd_resp_set_status(req, "401 Unauthorized");
        httpd_resp_send(req, "Unauthorized", HTTPD_RESP_USE_STRLEN);
        return ESP_FAIL;
    }
    esp_ota_handle_t update_handle = 0;
    const esp_partition_t *update_partition = esp_ota_get_next_update_partition(NULL);
    if (esp_ota_begin(update_partition, OTA_SIZE_UNKNOWN, &update_handle) != ESP_OK)
    {
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }

    char *buf = malloc(1024);
    if (!buf)
    {
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }

    int remaining = req->content_len;
    esp_err_t err = ESP_OK;

    while (remaining > 0)
    {
        int received = httpd_req_recv(req, buf, MIN(remaining, 1024));
        if (received <= 0)
        {
            err = ESP_FAIL;
            break;
        }
        esp_ota_write(update_handle, buf, received);
        remaining -= received;
    }

    free(buf);

    if (err != ESP_OK)
    {
        esp_ota_end(update_handle);
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }

    esp_ota_end(update_handle);
    esp_ota_set_boot_partition(update_partition);
    httpd_resp_send(req, "更新成功，设备将重启...", HTTPD_RESP_USE_STRLEN);
    vTaskDelay(pdMS_TO_TICKS(1000));
    esp_restart();
    return ESP_OK;
}

static esp_err_t api_data_handler(httpd_req_t *req)
{
    if (!is_authenticated(req))
    {
        httpd_resp_set_status(req, "401 Unauthorized");
        httpd_resp_send(req, "Unauthorized", HTTPD_RESP_USE_STRLEN);
        return ESP_OK;
    }
    char current_ssid[33] = "Not Connected";
    wifi_config_t wifi_cfg;
    if (esp_wifi_get_config(WIFI_IF_STA, &wifi_cfg) == ESP_OK)
    {
        if (wifi_cfg.sta.ssid[0] != '\0')
            strncpy(current_ssid, (char *)wifi_cfg.sta.ssid, sizeof(current_ssid) - 1);
    }
    char json_resp[600];
    snprintf(json_resp, sizeof(json_resp),
             "{\"ssid\":\"%s\",\"rssi\":%d,\"fps\":%d,\"brightness\":%ld,\"osd\":%d,\"time_osd\":%d,\"user\":\"%s\",\"ip\":\"%s\",\"ip6\":\"%s\",\"timezone\":\"%s\",\"udp_port\":%ld}",
             current_ssid, g_current_rssi, g_current_fps, g_current_brightness, g_show_osd, g_show_time_osd, g_admin_user, g_device_ip, g_device_ip6, g_timezone, g_udp_port);
    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, json_resp, HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}

static esp_err_t api_set_handler(httpd_req_t *req)
{
    if (!is_authenticated(req))
    {
        httpd_resp_set_status(req, "401 Unauthorized");
        httpd_resp_send(req, "Unauthorized", HTTPD_RESP_USE_STRLEN);
        return ESP_OK;
    }

    char buf[256];
    bool need_restart = false;

    if (httpd_req_get_url_query_str(req, buf, sizeof(buf)) == ESP_OK)
    {
        char param[64];
        if (httpd_query_key_value(buf, "brightness", param, sizeof(param)) == ESP_OK)
        {
            int32_t br = atoi(param);
            if (br < 0)
                br = 0;
            if (br > 100)
                br = 100;
            g_current_brightness = br;
            save_int_to_nvs("brightness", br);
            ledc_set_duty(LEDC_LOW_SPEED_MODE, LEDC_CHANNEL_0, (br * 255) / 100);
            ledc_update_duty(LEDC_LOW_SPEED_MODE, LEDC_CHANNEL_0);
        }
        if (httpd_query_key_value(buf, "osd", param, sizeof(param)) == ESP_OK)
        {
            g_show_osd = (atoi(param) == 1) ? 1 : 0;
            save_i8_to_nvs("show_osd", g_show_osd);
        }
        if (httpd_query_key_value(buf, "time", param, sizeof(param)) == ESP_OK)
        {
            g_show_time_osd = (atoi(param) == 1) ? 1 : 0;
            save_i8_to_nvs("show_time", g_show_time_osd);
        }
        if (httpd_query_key_value(buf, "timezone", param, sizeof(param)) == ESP_OK)
        {
            char decoded_tz[32];
            url_decode_safe(decoded_tz, param, sizeof(decoded_tz));
            strncpy(g_timezone, decoded_tz, sizeof(g_timezone) - 1);
            g_timezone[sizeof(g_timezone) - 1] = '\0';
            setenv("TZ", g_timezone, 1);
            tzset();
            save_str_to_nvs("tz_str", g_timezone);
        }

        if (httpd_query_key_value(buf, "udp_port", param, sizeof(param)) == ESP_OK)
        {
            int32_t port = atoi(param);
            if (g_udp_port != port && port > 0 && port < 65536)
            {
                g_udp_port = port;
                save_int_to_nvs("udp_port", port);
                need_restart = true;
            }
        }
    }

    if (need_restart)
    {
        httpd_resp_send(req, "RESTART", HTTPD_RESP_USE_STRLEN);
        vTaskDelay(pdMS_TO_TICKS(1000));
        esp_restart();
        return ESP_OK;
    }

    httpd_resp_send(req, "OK", HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}

static esp_err_t api_account_handler(httpd_req_t *req)
{
    if (!is_authenticated(req))
    {
        httpd_resp_set_status(req, "401 Unauthorized");
        httpd_resp_send(req, "Unauthorized", HTTPD_RESP_USE_STRLEN);
        return ESP_OK;
    }
    char buf[200];
    if (httpd_req_get_url_query_str(req, buf, sizeof(buf)) == ESP_OK)
    {
        char new_user_enc[64], new_pwd_enc[128];
        if (httpd_query_key_value(buf, "user", new_user_enc, sizeof(new_user_enc)) == ESP_OK &&
            httpd_query_key_value(buf, "pwd", new_pwd_enc, sizeof(new_pwd_enc)) == ESP_OK)
        {
            url_decode_safe(g_admin_user, new_user_enc, sizeof(g_admin_user));
            url_decode_safe(g_admin_pwd, new_pwd_enc, sizeof(g_admin_pwd));
            save_str_to_nvs("admin_user", g_admin_user);
            save_str_to_nvs("admin_pwd", g_admin_pwd);
        }
    }
    httpd_resp_send(req, "OK", HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}

static esp_err_t api_wifi_reset_handler(httpd_req_t *req)
{
    if (!is_authenticated(req))
    {
        httpd_resp_set_status(req, "401 Unauthorized");
        httpd_resp_send(req, "Unauthorized", HTTPD_RESP_USE_STRLEN);
        return ESP_OK;
    }
    network_prov_mgr_reset_wifi_provisioning();
    httpd_resp_send(req, "OK", HTTPD_RESP_USE_STRLEN);
    vTaskDelay(pdMS_TO_TICKS(1500));
    esp_restart();
    return ESP_OK;
}

static httpd_handle_t start_webserver(void)
{
    httpd_config_t config = HTTPD_DEFAULT_CONFIG();
    config.server_port = 80;
    config.max_uri_handlers = 16;
    httpd_handle_t server = NULL;
    if (httpd_start(&server, &config) == ESP_OK)
    {
        httpd_uri_t index_uri = {.uri = "/", .method = HTTP_GET, .handler = index_get_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &index_uri);
        httpd_uri_t login_uri = {.uri = "/login", .method = HTTP_GET, .handler = login_get_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &login_uri);
        httpd_uri_t do_login_uri = {.uri = "/do_login", .method = HTTP_GET, .handler = do_login_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &do_login_uri);
        httpd_uri_t api_data_uri = {.uri = "/api/data", .method = HTTP_GET, .handler = api_data_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &api_data_uri);
        httpd_uri_t api_set_uri = {.uri = "/api/set", .method = HTTP_GET, .handler = api_set_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &api_set_uri);
        httpd_uri_t api_acc_uri = {.uri = "/api/account", .method = HTTP_GET, .handler = api_account_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &api_acc_uri);
        httpd_uri_t api_wifi_reset_uri = {.uri = "/api/wifi_reset", .method = HTTP_POST, .handler = api_wifi_reset_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &api_wifi_reset_uri);
        httpd_uri_t ota_uri = {.uri = "/api/ota", .method = HTTP_POST, .handler = api_ota_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &ota_uri);
        httpd_uri_t do_restart_uri = {.uri = "/api/restart", .method = HTTP_POST, .handler = do_restart_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &do_restart_uri);
    }
    return server;
}

void time_sync_notification_cb(struct timeval *tv) {}

void init_time_sync(void)
{
    esp_sntp_setoperatingmode(SNTP_OPMODE_POLL);
    esp_sntp_setservername(0, "pool.ntp.org");
    esp_sntp_setservername(1, "time.stdtime.gov.tw");
    esp_sntp_setservername(2, "time.windows.com");
    sntp_set_time_sync_notification_cb(time_sync_notification_cb);
    esp_sntp_servermode_dhcp(1);
    esp_sntp_init();
    setenv("TZ", g_timezone, 1);
    tzset();
}

void app_main(void)
{
    esp_err_t ret = nvs_flash_init();
    if (ret == ESP_ERR_NVS_NO_FREE_PAGES || ret == ESP_ERR_NVS_NEW_VERSION_FOUND)
    {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ret = nvs_flash_init();
    }
    ESP_ERROR_CHECK(ret);

    // 使用官方通用标准方法注册多核通用的空闲钩子函数
    esp_register_freertos_idle_hook(vApplicationIdleHookCore);

    // 静默等待 50ms 让开机状态机稳定，随后精准测定 200ms 内的纯空闲最大计数值基准
    vTaskDelay(pdMS_TO_TICKS(50));
    g_core0_idle_cnt = 0;
    g_core1_idle_cnt = 0;
    vTaskDelay(pdMS_TO_TICKS(200));

    g_core0_max_idle = g_core0_idle_cnt;
    g_core1_max_idle = g_core1_idle_cnt;

    if (g_core0_max_idle == 0)
        g_core0_max_idle = 1;
    if (g_core1_max_idle == 0)
        g_core1_max_idle = 1;

    // 輸入端緩衝區使用硬體對齊分配 API，徹底滿足 SIMD 向量運算胃口
    for (int i = 0; i < RX_BUF_NUM; i++)
    {
        rx_frame_buf[i] = (uint8_t *)heap_caps_aligned_alloc(16, FRAME_BUF_SIZE, MALLOC_CAP_SPIRAM);
        if (!rx_frame_buf[i])
            while (1)
                vTaskDelay(1000);
    }

    // 增加 16 行的安全余量（320 * 16 * 2 = 10240 Bytes）
    for (int i = 0; i < DISPLAY_BUF_NUM; i++)
    {
        display_buf[i] = (uint8_t *)heap_caps_aligned_alloc(16, (LCD_PIXELS + LCD_WIDTH * 16) * 2, MALLOC_CAP_SPIRAM);

        if (!display_buf[i])
        {
            while (1)
                vTaskDelay(1000);
        }
    }

    jpeg_queue = xQueueCreate(RX_BUF_NUM, sizeof(rx_frame_t));
    free_queue = xQueueCreate(RX_BUF_NUM, sizeof(rx_frame_t));

    for (int i = 0; i < RX_BUF_NUM; i++)
    {
        rx_frame_t empty_frame = {.buf_ptr = rx_frame_buf[i], .len = 0};
        xQueueSend(free_queue, &empty_frame, 0);
    }

    ESP_ERROR_CHECK(esp_netif_init());
    s_wifi_event_group = xEventGroupCreate();
    ESP_ERROR_CHECK(esp_event_loop_create_default());

    esp_netif_t *sta_netif = esp_netif_create_default_wifi_sta();
    if (sta_netif != NULL)
    {
        uint8_t mac[6];
        esp_read_mac(mac, ESP_MAC_WIFI_STA);
        char hostname[32];
        snprintf(hostname, sizeof(hostname), "ESP32-RX-%02X%02X%02X", mac[3], mac[4], mac[5]);
        esp_netif_set_hostname(sta_netif, hostname);
        ESP_LOGI("WIFI", "Device Hostname set to: %s", hostname);
    }

    ESP_ERROR_CHECK(esp_event_handler_register(NETWORK_PROV_EVENT, ESP_EVENT_ANY_ID, &sys_event_handler, NULL));
    ESP_ERROR_CHECK(esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &sys_event_handler, NULL));
    ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, ESP_EVENT_ANY_ID, &sys_event_handler, NULL));

    lcd_hardware_init();
    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));

    network_prov_mgr_config_t config_prov = {.scheme = network_prov_scheme_ble, .scheme_event_handler = NETWORK_PROV_EVENT_HANDLER_NONE};
    ESP_ERROR_CHECK(network_prov_mgr_init(config_prov));

    bool provisioned = false;
    ESP_ERROR_CHECK(network_prov_mgr_is_wifi_provisioned(&provisioned));

    xTaskCreatePinnedToCore(jpeg_decode_display_task, "jpeg_task", 8192, NULL, 5, NULL, 1);

    if (!provisioned)
    {
        g_is_provisioning = true;
        network_prov_scheme_ble_set_service_uuid(g_prov_uuid);
        ESP_ERROR_CHECK(network_prov_mgr_start_provisioning(NETWORK_PROV_SECURITY_1, (const void *)g_prov_pop, "PROV_DISPLAY", NULL));
    }
    else
    {
        network_prov_mgr_deinit();
        ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
        ESP_ERROR_CHECK(esp_wifi_set_protocol(WIFI_IF_STA, WIFI_PROTOCOL_11N));
        ESP_ERROR_CHECK(esp_wifi_start());
    }

    xEventGroupWaitBits(s_wifi_event_group, WIFI_CONNECTED_BIT, pdFALSE, pdFALSE, portMAX_DELAY);
    ESP_ERROR_CHECK(esp_wifi_set_ps(WIFI_PS_NONE));
    init_time_sync();

    xTaskCreatePinnedToCore(udp_receiver_task, "udp_task", 8192, NULL, 8, NULL, 0);
    start_webserver();
}