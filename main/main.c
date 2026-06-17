#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <errno.h> // 用于 Socket 错误码处理
#include <dirent.h>
#include <sys/stat.h>
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

#ifndef PROJECT_VER
#define PROJECT_VER "unknown"
#endif

// 空闲钩子组件头文件
#include "esp_freertos_hooks.h"

// 蓝牙配网库
#include "network_provisioning/manager.h"
#include "network_provisioning/scheme_ble.h"

// 硬件核心驱动
#include "esp_lcd_panel_io.h"
#include "esp_lcd_panel_ops.h"
#include "esp_lcd_ili9341.h"
#include "driver/gpio.h"
#include "driver/i2c_master.h"
#include "driver/spi_master.h"
#include "driver/ledc.h"
#include "led_strip.h"
#include "esp_vfs_fat.h"
#include "sdmmc_cmd.h"
#include "driver/sdmmc_host.h"

#include "esp_idf_version.h"
#if ESP_IDF_VERSION >= ESP_IDF_VERSION_VAL(5, 0, 0)
#include "driver/i2s_std.h"
#else
#include "driver/i2s.h"
#endif

#include "esp_adc/adc_oneshot.h"
#include "esp_adc/adc_cali.h"
#include "esp_adc/adc_cali_scheme.h"

#if ESP_IDF_VERSION >= ESP_IDF_VERSION_VAL(5, 3, 0)
#define MY_ADC_ATTEN ADC_ATTEN_DB_12
#else
#define MY_ADC_ATTEN ADC_ATTEN_DB_11
#endif

// 高性能解码器
#include "esp_jpeg_dec.h"
#include "esp_jpeg_enc.h"

#define FRAME_BUF_SIZE 122880

// 屏幕分辨率
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

#define DISPLAY_BUF_NUM 12
#define RX_BUF_NUM 12

// SD 卡引脚定义 (请根据你的硬件连接修改)
#define SD_PIN_CLK 38
#define SD_PIN_CMD 40
#define SD_PIN_D0 39
#define SD_PIN_D1 41
#define SD_PIN_D2 48
#define SD_PIN_D3 47

#define MOUNT_POINT "/sdcard"

#define RGB_LED_PIN 42

// Audio I2S Pins
#define PA_EN_PIN 1
#define I2S_MCLK_PIN 4
#define I2S_BCLK_PIN 5
#define I2S_DOUT_PIN 8
#define I2S_LRCK_PIN 7
#define I2S_DIN_PIN -1 // 不需要麦克风输入，设为 -1 释放引脚

#define I2C_SDA_PIN 16
#define I2C_SCL_PIN 15

// ==================== 全局状态与持久化配置 ====================
volatile int g_current_fps = 0;
volatile int8_t g_current_rssi = 0;

volatile int32_t g_current_brightness = 60;
volatile int32_t g_current_volume = 80;
volatile int8_t g_show_osd = 1;
volatile bool g_show_time_osd = true;
char g_admin_user[32] = "admin";
char g_admin_pwd[64] = "123456";
char g_device_ip[20] = "0.0.0.0";
char g_device_ip6[40] = "0000:0000:0000:0000:0000:0000:0000:0000";
char g_timezone[32] = "CST-8";

volatile int32_t g_udp_port = 8888;

volatile bool g_is_provisioning = false;
volatile bool g_sd_card_mounted = false;
volatile bool g_is_playing_from_sd = false;
volatile bool g_stop_playback = false;
volatile bool g_pause_playback = false;

volatile uint32_t g_sd_file_size = 0;
volatile uint32_t g_sd_current_pos = 0;
volatile int g_seek_permille = -1; // 用千分比来做细粒度的精度控制 (0~1000)

volatile uint8_t g_rgb_r = 0, g_rgb_g = 0, g_rgb_b = 0;
char g_syslog_host[64] = "";
volatile int8_t g_syslog_enable = 0;

volatile int8_t g_audio_enable = 1;

// 网络质量监控
volatile uint32_t g_video_packet_drop_count = 0;
volatile uint32_t g_audio_plc_count = 0; // 音频丢包补偿 (Packet Loss Concealment) 触发次数

#if ESP_IDF_VERSION >= ESP_IDF_VERSION_VAL(5, 0, 0)
static i2s_chan_handle_t tx_chan;
#endif

static TaskHandle_t s_sd_playback_task_handle = NULL;
static char s_playback_filename[128];

static i2c_master_dev_handle_t es8311_dev_handle = NULL;

volatile int g_battery_voltage_mv = 0;
volatile int g_battery_percentage = 0;
static adc_oneshot_unit_handle_t adc1_handle = NULL;
static adc_cali_handle_t adc1_cali_handle = NULL;
static bool do_calibration = false;

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

static led_strip_handle_t s_led_strip = NULL;
static sdmmc_card_t *s_card;

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
    draw_string_8x8_transparent_fast(full_frame, 20, 90, "Device Name : PROV_DISPLAY", 0xE007);
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
        if (netif)
        {
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

        int32_t saved_vol = 80;
        if (nvs_get_i32(my_handle, "volume", &saved_vol) == ESP_OK)
            g_current_volume = saved_vol;

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

        uint8_t u8_tmp = 0;
        if (nvs_get_u8(my_handle, "rgb_r", &u8_tmp) == ESP_OK)
            g_rgb_r = u8_tmp;
        if (nvs_get_u8(my_handle, "rgb_g", &u8_tmp) == ESP_OK)
            g_rgb_g = u8_tmp;
        if (nvs_get_u8(my_handle, "rgb_b", &u8_tmp) == ESP_OK)
            g_rgb_b = u8_tmp;

        int8_t saved_audio = 1;
        if (nvs_get_i8(my_handle, "audio_enable", &saved_audio) == ESP_OK)
            g_audio_enable = saved_audio;

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
static void save_u8_to_nvs(const char *key, uint8_t val)
{
    nvs_handle_t my_handle;
    if (nvs_open("storage", NVS_READWRITE, &my_handle) == ESP_OK)
    {
        nvs_set_u8(my_handle, key, val);
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

    esp_lcd_panel_io_spi_config_t io_config = {.dc_gpio_num = TFT_DC, .cs_gpio_num = TFT_CS, .pclk_hz = 58 * 1000 * 1000, .lcd_cmd_bits = 8, .lcd_param_bits = 8, .spi_mode = 0, .trans_queue_depth = 20};
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

static void sd_card_init(void)
{
    // If already mounted, check if it's still accessible.
    if (g_sd_card_mounted)
    {
        uint64_t total, free;
        if (esp_vfs_fat_info(MOUNT_POINT, &total, &free) == ESP_OK)
        {
            return; // Still OK, do nothing.
        }
        else
        {
            ESP_LOGW("SD", "Card was mounted but is now inaccessible. Unmounting.");
            esp_vfs_fat_sdcard_unmount(MOUNT_POINT, s_card);
            g_sd_card_mounted = false;
        }
    }

    esp_err_t ret;
    esp_vfs_fat_sdmmc_mount_config_t mount_config = {
        .format_if_mount_failed = true,
        .max_files = 5,
        .allocation_unit_size = 16 * 1024};

    sdmmc_host_t host = SDMMC_HOST_DEFAULT();
    host.max_freq_khz = SDMMC_FREQ_DEFAULT;

    sdmmc_slot_config_t slot_config = SDMMC_SLOT_CONFIG_DEFAULT();
    slot_config.width = 4; // 4-line mode
    slot_config.clk = SD_PIN_CLK;
    slot_config.cmd = SD_PIN_CMD;
    slot_config.d0 = SD_PIN_D0;
    slot_config.d1 = SD_PIN_D1;
    slot_config.d2 = SD_PIN_D2;
    slot_config.d3 = SD_PIN_D3;

    ret = esp_vfs_fat_sdmmc_mount(MOUNT_POINT, &host, &slot_config, &mount_config, &s_card);

    if (ret != ESP_OK)
    {
        // Don't log error if it's just not found, that's expected on boot without card.
        if (ret != ESP_ERR_NOT_FOUND)
        {
            ESP_LOGE("SD", "Failed to mount SD card (%s).", esp_err_to_name(ret));
        }
        g_sd_card_mounted = false;
    }
    else
    {
        ESP_LOGI("SD", "SD card mounted at %s", MOUNT_POINT);
        sdmmc_card_print_info(stdout, s_card);
        g_sd_card_mounted = true;
    }
}

static void rgb_led_init(void)
{
    led_strip_config_t strip_config = {
        .strip_gpio_num = RGB_LED_PIN,
        .max_leds = 1, // 这里假定只有一颗灯珠，如有多颗可自行修改
    };
    led_strip_rmt_config_t rmt_config = {
        .resolution_hz = 10 * 1000 * 1000, // 10MHz
    };
    if (led_strip_new_rmt_device(&strip_config, &rmt_config, &s_led_strip) == ESP_OK)
    {
        led_strip_set_pixel(s_led_strip, 0, g_rgb_r, g_rgb_g, g_rgb_b);
        led_strip_refresh(s_led_strip);
    }
}

static esp_err_t es8311_write_reg(uint8_t reg, uint8_t val)
{
    if (!es8311_dev_handle)
        return ESP_FAIL;
    uint8_t data[2] = {reg, val};
    esp_err_t err = i2c_master_transmit(es8311_dev_handle, data, sizeof(data), pdMS_TO_TICKS(100));
    if (err != ESP_OK)
    {
        ESP_LOGE("ES8311", "I2C Write Failed! Reg: 0x%02X, Err: %s", reg, esp_err_to_name(err));
    }
    return err;
}

static void es8311_codec_init(void)
{
    i2c_master_bus_config_t i2c_bus_cfg = {
        .i2c_port = I2C_NUM_0,
        .sda_io_num = I2C_SDA_PIN,
        .scl_io_num = I2C_SCL_PIN,
        .clk_source = I2C_CLK_SRC_DEFAULT,
        .glitch_ignore_cnt = 7,
        .flags.enable_internal_pullup = true,
    };
    i2c_master_bus_handle_t bus_handle;
    ESP_ERROR_CHECK(i2c_new_master_bus(&i2c_bus_cfg, &bus_handle));

    i2c_device_config_t i2c_dev_cfg = {
        .dev_addr_length = I2C_ADDR_BIT_LEN_7,
        .device_address = 0x18,
        .scl_speed_hz = 400000,
    };
    ESP_ERROR_CHECK(i2c_master_bus_add_device(bus_handle, &i2c_dev_cfg, &es8311_dev_handle));

    // ES8311 寄存器初始化序列 (44.1kHz, 16bit, Slave 接收模式)
    // 完全复刻官方库 es8311_init 与 es8311_sample_frequency_config 逻辑
    es8311_write_reg(0x00, 0x1F);
    vTaskDelay(pdMS_TO_TICKS(30)); // Reset
    es8311_write_reg(0x00, 0x00);
    es8311_write_reg(0x00, 0x80); // Power-on command (CSM=1)

    es8311_write_reg(0x01, 0x3F); // Clock config: Enable all clocks
    es8311_write_reg(0x02, 0x00); // MCLK pre-div=1, pre-multi=1x
    es8311_write_reg(0x03, 0x10); // fs_mode=ss, adc_osr=16
    es8311_write_reg(0x04, 0x10); // dac_osr=16
    es8311_write_reg(0x05, 0x00); // adc_div=1, dac_div=1
    es8311_write_reg(0x06, 0x03); // bclk_div=4
    es8311_write_reg(0x07, 0x00); // lrck_h=0
    es8311_write_reg(0x08, 0xFF); // lrck_l=255

    es8311_write_reg(0x09, 0x0C); // SDP IN: 16-bit
    es8311_write_reg(0x0A, 0x0C); // SDP OUT: 16-bit

    es8311_write_reg(0x0D, 0x01); // Power up analog circuitry
    es8311_write_reg(0x0E, 0x02); // Enable analog PGA, enable ADC modulator
    es8311_write_reg(0x12, 0x00); // Power-up DAC
    es8311_write_reg(0x13, 0x10); // Enable output to HP drive (Crucial!)
    es8311_write_reg(0x1C, 0x6A); // ADC Equalizer bypass
    es8311_write_reg(0x37, 0x08); // Bypass DAC equalizer

    es8311_write_reg(0x31, 0x00); // DAC unmute

    es8311_write_reg(0x32, 0); // Set initial volume to 0 to prevent pop
}

static void audio_hardware_init(void)
{
    // PA 使能引脚初始化 (低电平使能)
    gpio_config_t io_conf = {
        .pin_bit_mask = (1ULL << PA_EN_PIN),
        .mode = GPIO_MODE_OUTPUT,
        .pull_up_en = 0,
        .pull_down_en = 0,
        .intr_type = GPIO_INTR_DISABLE};
    gpio_config(&io_conf);
    gpio_set_level(PA_EN_PIN, 1); // Keep PA disabled initially to prevent pop

#if ESP_IDF_VERSION >= ESP_IDF_VERSION_VAL(5, 0, 0)
    i2s_chan_config_t chan_cfg = I2S_CHANNEL_DEFAULT_CONFIG(I2S_NUM_AUTO, I2S_ROLE_MASTER);
    ESP_ERROR_CHECK(i2s_new_channel(&chan_cfg, &tx_chan, NULL));

    i2s_std_config_t std_cfg = {
        .clk_cfg = I2S_STD_CLK_DEFAULT_CONFIG(44100),
        .slot_cfg = I2S_STD_PHILIPS_SLOT_DEFAULT_CONFIG(I2S_DATA_BIT_WIDTH_16BIT, I2S_SLOT_MODE_STEREO),
        .gpio_cfg = {
            .mclk = I2S_MCLK_PIN,
            .bclk = I2S_BCLK_PIN,
            .ws = I2S_LRCK_PIN,
            .dout = I2S_DOUT_PIN,
            .din = I2S_DIN_PIN,
            .invert_flags = {
                .mclk_inv = false,
                .bclk_inv = false,
                .ws_inv = false,
            },
        },
    };
    ESP_ERROR_CHECK(i2s_channel_init_std_mode(tx_chan, &std_cfg));
    ESP_ERROR_CHECK(i2s_channel_enable(tx_chan));
#else
    i2s_config_t i2s_config = {
        .mode = I2S_MODE_MASTER | I2S_MODE_TX,
        .sample_rate = 44100,
        .bits_per_sample = I2S_BITS_PER_SAMPLE_16BIT,
        .channel_format = I2S_CHANNEL_FMT_RIGHT_LEFT,
        .communication_format = I2S_COMM_FORMAT_STAND_I2S,
        .intr_alloc_flags = ESP_INTR_FLAG_LEVEL1,
        .dma_buf_count = 6,
        .dma_buf_len = 512,
        .use_apll = false, // ESP32-S3 硬件不支持 APLL，使用默认时钟即可
        .tx_desc_auto_clear = true};
    i2s_pin_config_t pin_config = {
        .mck_io_num = I2S_MCLK_PIN,
        .bck_io_num = I2S_BCLK_PIN,
        .ws_io_num = I2S_LRCK_PIN,
        .data_out_num = I2S_DOUT_PIN,
        .data_in_num = I2S_DIN_PIN};
    ESP_ERROR_CHECK(i2s_driver_install(I2S_NUM_0, &i2s_config, 0, NULL));
    ESP_ERROR_CHECK(i2s_set_pin(I2S_NUM_0, &pin_config));
    ESP_ERROR_CHECK(i2s_zero_dma_buffer(I2S_NUM_0));
#endif

    // 必须在 I2S 时钟 (MCLK) 启动后，再初始化 ES8311，否则它可能无法响应配置
    es8311_codec_init();

    // After codec is configured and stable, enable the PA and ramp up volume
    if (g_audio_enable)
    {
        gpio_set_level(PA_EN_PIN, 0); // Enable PA (low level)
        ESP_LOGI("AUDIO", "Ramping up volume for soft start...");
        for (int i = 0; i <= g_current_volume; i++)
        {
            uint8_t vol_reg = i == 0 ? 0 : ((i * 256 / 100) - 1);
            es8311_write_reg(0x32, vol_reg);
            vTaskDelay(pdMS_TO_TICKS(3)); // 3ms per step for a smoother fade-in
        }
        ESP_LOGI("AUDIO", "Volume ramp-up complete.");
    }
}

static void battery_adc_init(void)
{
    adc_oneshot_unit_init_cfg_t init_config = {
        .unit_id = ADC_UNIT_1,
    };
    ESP_ERROR_CHECK(adc_oneshot_new_unit(&init_config, &adc1_handle));

    adc_oneshot_chan_cfg_t config = {
        .bitwidth = ADC_BITWIDTH_DEFAULT,
        .atten = MY_ADC_ATTEN,
    };
    ESP_ERROR_CHECK(adc_oneshot_config_channel(adc1_handle, ADC_CHANNEL_8, &config)); // IO9 是 ESP32-S3 的 ADC1_CH8

    adc_cali_curve_fitting_config_t cali_config = {
        .unit_id = ADC_UNIT_1,
        .chan = ADC_CHANNEL_8,
        .atten = MY_ADC_ATTEN,
        .bitwidth = ADC_BITWIDTH_DEFAULT,
    };
    if (adc_cali_create_scheme_curve_fitting(&cali_config, &adc1_cali_handle) == ESP_OK)
    {
        do_calibration = true;
    }
}

void audio_receiver_task(void *pvParameters)
{
    int rcv_buf_size = 65536; // 增大 Socket 缓冲区，应对突发数据
    // 将超时时间从 1 秒大幅缩短至 100 毫秒，以便快速响应网络丢包
    struct timeval timeout = {.tv_sec = 0, .tv_usec = 100000};
    uint8_t *read_buf = (uint8_t *)malloc(4096);
    // 创建一个用于“补帧”的静音包
    uint8_t *silence_buf = (uint8_t *)calloc(1, 3528); // 44.1kHz 16bit 立体声 20ms 的数据量

    if (!read_buf || !silence_buf)
    {
        ESP_LOGE("AUDIO", "Failed to allocate audio buffers");
        if (read_buf)
            free(read_buf);
        if (silence_buf)
            free(silence_buf);
        vTaskDelete(NULL);
    }
    static bool first_audio = true;

    while (1)
    {
        if (g_is_playing_from_sd)
        {
            // 如果正在播放 SD 卡，暂停网络音频接收，让出 I2S 资源
            vTaskDelay(pdMS_TO_TICKS(500));
            continue;
        }

        xEventGroupWaitBits(s_wifi_event_group, WIFI_CONNECTED_BIT, pdFALSE, pdTRUE, portMAX_DELAY);
        int sock = socket(AF_INET6, SOCK_DGRAM, IPPROTO_IP);
        if (sock < 0)
        {
            vTaskDelay(pdMS_TO_TICKS(1000));
            continue;
        }

        int v6only = 0;
        setsockopt(sock, IPPROTO_IPV6, IPV6_V6ONLY, &v6only, sizeof(v6only));
        setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &rcv_buf_size, sizeof(rcv_buf_size));
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

        struct sockaddr_in6 dest_addr = {0};
        dest_addr.sin6_family = AF_INET6;
        dest_addr.sin6_port = htons(g_udp_port + 1); // 独立接收音频端口 (视频端口 + 1)
        dest_addr.sin6_addr = in6addr_any;

        if (bind(sock, (struct sockaddr *)&dest_addr, sizeof(dest_addr)) < 0)
        {
            close(sock);
            vTaskDelay(pdMS_TO_TICKS(1000));
            continue;
        }
        while (1)
        {
            // 修复：同时监听 SD 卡播放状态，一旦开始播放立刻跳出内层循环，关闭 Socket 并释放资源
            if ((xEventGroupGetBits(s_wifi_event_group) & WIFI_CONNECTED_BIT) == 0 || g_is_playing_from_sd)
                break;
            int len = recvfrom(sock, read_buf, 4096, 0, NULL, NULL);

            if (len > 0)
            {
                if (first_audio)
                {
                    ESP_LOGW("AUDIO", "✅ First audio packet received! Length: %d bytes", len);
                    first_audio = false; // 打印日志以确认网络包已成功到达音频端口
                }
                if (g_audio_enable)
                {
#if ESP_IDF_VERSION >= ESP_IDF_VERSION_VAL(5, 0, 0)
                    size_t bytes_written = 0;
                    // 使用有限超时，避免音频任务被 I2S 长时间阻塞
                    i2s_channel_write(tx_chan, read_buf, len, &bytes_written, pdMS_TO_TICKS(100));
#else
                    size_t bytes_written = 0;
                    i2s_write(I2S_NUM_0, read_buf, len, &bytes_written, pdMS_TO_TICKS(100));
#endif
                }
            }
            else if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                g_audio_plc_count++; // 累加丢包补偿计数
                // 超时，说明网络发生抖动或丢包，主动写入 20ms 静音数据来填充 I2S 硬件缓冲
                // 避免 DMA 缓冲“饿死”产生爆音，实现“丢包不掉线”的平滑过渡
                if (g_audio_enable && !first_audio)
                {
#if ESP_IDF_VERSION >= ESP_IDF_VERSION_VAL(5, 0, 0)
                    size_t bytes_written = 0;
                    i2s_channel_write(tx_chan, silence_buf, 3528, &bytes_written, pdMS_TO_TICKS(100));
#else
                    size_t bytes_written = 0;
                    i2s_write(I2S_NUM_0, silence_buf, 3528, &bytes_written, pdMS_TO_TICKS(100));
#endif
                }
            }
            else
            {
                // 发生其他 Socket 错误
                ESP_LOGE("AUDIO", "Socket recv error: %d", errno);
                break; // 跳出内层循环，重新建立 Socket
            }
        }
        close(sock);
        first_audio = true; // 重置标志位
        vTaskDelay(pdMS_TO_TICKS(500)); // 避免错误发生后疯狂重连
    }
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
        if (xQueueReceive(jpeg_queue, &current_frame, pdMS_TO_TICKS(5)) == pdTRUE)
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
                    static char cached_osd_str[32] = {0}, cached_time_str[32] = {0}, cached_heap_str[32] = {0}, cached_cpu_str[32] = {0}, cached_net_str[32] = {0}, cached_bssid_str[32] = {0}, cached_sd_str[32] = {0}, cached_bat_str[16] = {0};
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

                        if (g_sd_card_mounted)
                        {
                            uint64_t sd_total = 0, sd_free = 0;
                            esp_vfs_fat_info(MOUNT_POINT, &sd_total, &sd_free);
                            uint32_t free_mb = sd_free / (1024 * 1024);
                            snprintf(cached_sd_str, sizeof(cached_sd_str), "SD:%s %luMB", g_is_playing_from_sd ? "PLAY" : "IDLE", (unsigned long)free_mb);
                        }
                        else
                        {
                            cached_sd_str[0] = '\0';
                        }
                        snprintf(cached_bat_str, sizeof(cached_bat_str), "BAT:%d%%", g_battery_percentage);

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
                        if (cached_bat_str[0] != '\0')
                        {
                            draw_string_8x8_transparent_fast(full_frame, 2, LCD_HEIGHT - 70, cached_bat_str, 0x07E0);
                        }
                        if (cached_sd_str[0] != '\0')
                        {
                            uint16_t sd_color = 0x07E0; // 默认绿色
                            if (g_is_playing_from_sd)
                            {
                                sd_color = ((xTaskGetTickCount() / pdMS_TO_TICKS(500)) % 2 == 0) ? 0x07E0 : 0xFFFF; // 播放时绿白闪烁
                            }
                            draw_string_8x8_transparent_fast(full_frame, 2, LCD_HEIGHT - 60, cached_sd_str, sd_color);
                        }
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

void sd_playback_task(void *pvParameters)
{
    uint8_t *file_read_buf = (uint8_t *)malloc(188 * 64); // ~12KB buffer
    if (!file_read_buf)
    {
        ESP_LOGE("PLAY", "Failed to allocate file read buffer!");
        vTaskDelete(NULL);
    }

    rx_frame_t current_rx_frame;
    bool has_active_buffer = false;
    uint32_t current_frame_len = 0;
    bool frame_corrupted = true;
    int8_t last_video_cc = -1;
    uint16_t video_pid = 0x2000;
    int8_t last_audio_cc = -1;
    uint16_t audio_pid = 0x2000;

    while (1)
    {
        ulTaskNotifyTake(pdTRUE, portMAX_DELAY); // Wait for a play command

        g_is_playing_from_sd = true;
        g_stop_playback = false;
        g_pause_playback = false;
        g_seek_permille = -1;
        video_pid = 0x2000;
        audio_pid = 0x2000;
        frame_corrupted = true;
        has_active_buffer = false;

        char filepath[200];
        snprintf(filepath, sizeof(filepath), "%s/%s", MOUNT_POINT, s_playback_filename);

        FILE *f = fopen(filepath, "r");
        if (!f)
        {
            ESP_LOGE("PLAY", "Failed to open %s", filepath);
            g_is_playing_from_sd = false;
            continue;
        }

        fseek(f, 0, SEEK_END);
        g_sd_file_size = ftell(f);
        fseek(f, 0, SEEK_SET);
        g_sd_current_pos = 0;

        ESP_LOGI("PLAY", "Playing %s...", filepath);

        while (!g_stop_playback)
        {
            if (g_seek_permille >= 0)
            {
                uint32_t target_pos = (g_sd_file_size / 1000) * g_seek_permille;
                target_pos = (target_pos / 188) * 188; // 强制 188 字节对齐，完美匹配 TS 格式包头
                fseek(f, target_pos, SEEK_SET);
                g_seek_permille = -1;
                g_sd_current_pos = ftell(f);
                frame_corrupted = true;
                has_active_buffer = false;
            }

            if (g_pause_playback)
            {
                vTaskDelay(pdMS_TO_TICKS(50));
                continue;
            }

            size_t bytes_read = fread(file_read_buf, 1, 188 * 64, f);
            if (bytes_read == 0)
                break; // End of file

            g_sd_current_pos = ftell(f);

            for (int offset = 0; offset <= (int)bytes_read - 188; offset += 188)
            {
                if (g_stop_playback)
                    break;

                uint8_t *ts = &file_read_buf[offset];
                if (ts[0] != 0x47)
                    continue;

                uint16_t pid = ((ts[1] & 0x1F) << 8) | ts[2];
                uint8_t pusi = (ts[1] & 0x40) >> 6;
                uint8_t afc = (ts[3] & 0x30) >> 4;
                uint8_t cc = ts[3] & 0x0F;

                if (afc == 0 || afc == 2)
                    continue;

                int payload_offset = 4;
                if (afc == 3)
                    payload_offset += 1 + ts[4];
                if (payload_offset >= 188)
                    continue;

                uint8_t *payload = &ts[payload_offset];
                int payload_len = 188 - payload_offset;

                if (pusi)
                {
                    if (payload_len >= 6 && payload[0] == 0x00 && payload[1] == 0x00 && payload[2] == 0x01)
                    {
                        if ((payload[3] & 0xF0) == 0xE0)
                        { // Video PES (0xE0 ~ 0xEF)
                            video_pid = pid;
                            int pes_header_len = 9 + payload[8];
                            if (payload_len > pes_header_len)
                            {
                                payload += pes_header_len;
                                payload_len -= pes_header_len;
                                if (!has_active_buffer && xQueueReceive(free_queue, &current_rx_frame, 0) == pdTRUE)
                                {
                                    has_active_buffer = true;
                                }
                                if (has_active_buffer)
                                {
                                    current_frame_len = 0;
                                    frame_corrupted = false;
                                    last_video_cc = cc;
                                }
                                else
                                {
                                    frame_corrupted = true;
                                }
                            }
                            else
                                continue;
                        }
                        else if ((payload[3] & 0xE0) == 0xC0 || payload[3] == 0xBD)
                        { // Audio PES (0xC0~0xDF) 或 LPCM私有流 (0xBD)
                            if (audio_pid == 0x2000 || audio_pid == pid)
                            { // 锁定首个音频 PID
                                audio_pid = pid;
                                last_audio_cc = cc;
                                if (payload_len > 8)
                                { // 确保有足够长度读取 payload[8]
                                    int pes_header_len = 9 + payload[8];
                                    if (payload_len > pes_header_len)
                                    {
                                        payload += pes_header_len;
                                        payload_len -= pes_header_len;
                                        if (g_audio_enable)
                                        {
#if ESP_IDF_VERSION >= ESP_IDF_VERSION_VAL(5, 0, 0)
                                            size_t bytes_written = 0;
                                            i2s_channel_write(tx_chan, payload, payload_len, &bytes_written, portMAX_DELAY);
#else
                                            size_t bytes_written = 0;
                                            i2s_write(I2S_NUM_0, payload, payload_len, &bytes_written, portMAX_DELAY);
#endif
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                if (pid == video_pid && has_active_buffer && !frame_corrupted)
                {
                    if (!pusi)
                    {
                        int expected_cc = (last_video_cc + 1) & 0x0F;
                        if (cc != expected_cc)
                        {
                            ESP_LOGW("PLAY", "Video packet drop! Expected CC %d, got %d.", expected_cc, cc);
                            frame_corrupted = true;
                        }
                        last_video_cc = cc;
                    }
                    if (!frame_corrupted)
                    {
                        if (current_frame_len + payload_len <= FRAME_BUF_SIZE)
                        {
                            memcpy(current_rx_frame.buf_ptr + current_frame_len, payload, payload_len);
                            current_frame_len += payload_len;
                            if (current_frame_len >= 2 && current_rx_frame.buf_ptr[current_frame_len - 2] == 0xFF && current_rx_frame.buf_ptr[current_frame_len - 1] == 0xD9)
                            {
                                current_rx_frame.len = current_frame_len;
                                if (xQueueSend(jpeg_queue, &current_rx_frame, 0) == pdTRUE)
                                {
                                    has_active_buffer = false;
                                }
                                frame_corrupted = true;
                            }
                        }
                        else
                        {
                            ESP_LOGW("PLAY", "Video frame overflow!");
                            frame_corrupted = true;
                        }
                    }
                }
                if (pid == audio_pid && !pusi)
                {
                    int expected_cc = (last_audio_cc + 1) & 0x0F;
                    if (cc != expected_cc)
                    {
                        ESP_LOGW("PLAY", "Audio packet drop! Expected CC %d, got %d.", expected_cc, cc);
                    }
                    if (g_audio_enable)
                    {
#if ESP_IDF_VERSION >= ESP_IDF_VERSION_VAL(5, 0, 0)
                        size_t bytes_written = 0;
                        i2s_channel_write(tx_chan, payload, payload_len, &bytes_written, portMAX_DELAY);
#else
                        size_t bytes_written = 0;
                        i2s_write(I2S_NUM_0, payload, payload_len, &bytes_written, portMAX_DELAY);
#endif
                    }
                    last_audio_cc = cc;
                }
            }
        }

        fclose(f);
        g_sd_file_size = 0;
        g_sd_current_pos = 0;
        if (has_active_buffer)
        {
            xQueueSend(free_queue, &current_rx_frame, 0);
        }
        ESP_LOGI("PLAY", "Playback finished.");
        g_is_playing_from_sd = false;
    }
    free(file_read_buf);
    vTaskDelete(NULL);
}

void video_receiver_task(void *pvParameters)
{
    int rcv_buf_size = 65536;
    struct timeval timeout = {.tv_sec = 1, .tv_usec = 0};

    uint8_t *read_buf = (uint8_t *)malloc(4096); // 改用堆内存并增大到 4KB，防止 Jumbo Frame 或合并的 UDP 包被截断
    if (!read_buf) {
        ESP_LOGE("UDP", "Alloc failed");
        vTaskDelete(NULL);
    }

    while (1)
    {
        if (g_is_playing_from_sd)
        {
            // SD card playback is active, pause network reception.
            vTaskDelay(pdMS_TO_TICKS(500));
            continue;
        }

        xEventGroupWaitBits(s_wifi_event_group, WIFI_CONNECTED_BIT, pdFALSE, pdTRUE, portMAX_DELAY);
        // 改為 AF_INET6
        int sock = socket(AF_INET6, SOCK_DGRAM, IPPROTO_IP);
        if (sock < 0)
        {
            vTaskDelay(pdMS_TO_TICKS(1000));
            continue;
        }

        // 關閉 IPV6_V6ONLY，開啟 Dual Stack (雙棧) 模式，使其能同時接收 IPv4 與 IPv6 推流
        int v6only = 0;
        setsockopt(sock, IPPROTO_IPV6, IPV6_V6ONLY, &v6only, sizeof(v6only));

        setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &rcv_buf_size, sizeof(rcv_buf_size));
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

        // 使用 sockaddr_in6 結構體
        struct sockaddr_in6 dest_addr = {0};
        dest_addr.sin6_family = AF_INET6;
        dest_addr.sin6_port = htons(g_udp_port);
        dest_addr.sin6_addr = in6addr_any; // in6addr_any 同時包含 0.0.0.0 與 ::

        if (bind(sock, (struct sockaddr *)&dest_addr, sizeof(dest_addr)) < 0)
        {
            close(sock);
            vTaskDelay(pdMS_TO_TICKS(1000));
            continue;
        }

        uint32_t current_frame_len = 0;
        rx_frame_t current_rx_frame;
        bool has_active_buffer = false;

        // TS 解析狀態變數
        uint16_t video_pid = 0x2000; // 初始無效 PID
        int8_t last_cc = -1;
        bool frame_corrupted = true; // 預設丟棄，直到看見新幀的起點

        while (1)
        {
            // 同时监听 SD 卡播放状态，一旦开始播放立刻跳出内层循环让出网络和队列资源
            if ((xEventGroupGetBits(s_wifi_event_group) & WIFI_CONNECTED_BIT) == 0 || g_is_playing_from_sd)
                break;

            // 接收 UDP 數據
            int len = recvfrom(sock, read_buf, 4096, 0, NULL, NULL);

            if (len > 0)
            {
                g_rx_bytes_1s += len;

                // 遍歷 UDP 封包內的每一個 188 Bytes TS 封包
                for (int offset = 0; offset <= len - 188; offset += 188)
                {
                    uint8_t *ts = &read_buf[offset];

                    // TS 同步字節檢查
                    if (ts[0] != 0x47)
                        continue;

                    uint16_t pid = ((ts[1] & 0x1F) << 8) | ts[2];
                    uint8_t pusi = (ts[1] & 0x40) >> 6; // Payload Unit Start Indicator (新幀開始標誌)
                    uint8_t afc = (ts[3] & 0x30) >> 4;  // Adaptation Field Control
                    uint8_t cc = ts[3] & 0x0F;          // Continuity Counter (0-15 循環)

                    if (afc == 0 || afc == 2)
                        continue; // 沒有 Payload 的包直接跳過

                    int payload_offset = 4;
                    if (afc == 3)
                    {
                        payload_offset += 1 + ts[4]; // 跳過 Adaptation Field
                    }
                    if (payload_offset >= 188)
                        continue;

                    uint8_t *payload = &ts[payload_offset];
                    int payload_len = 188 - payload_offset;

                    // 如果是新的一幀 (PES Packet Start)
                    if (pusi)
                    {
                        // 尋找影片 PES Header (0x00 0x00 0x01 0xE0)
                        if (payload_len >= 9 && payload[0] == 0x00 && payload[1] == 0x00 && payload[2] == 0x01 && payload[3] == 0xE0)
                        {
                            video_pid = pid; // 自動鎖定影片軌的 PID

                        // 【关键修复】如果上一帧因为结尾有 padding (填充字符) 导致 FF D9 判断失败，在这里利用 PUSI (新帧开始) 补救提交！
                        if (has_active_buffer && !frame_corrupted && current_frame_len > 1024)
                        {
                            current_rx_frame.len = current_frame_len;
                            if (xQueueSend(jpeg_queue, &current_rx_frame, 0) == pdTRUE)
                            {
                                has_active_buffer = false;
                            }
                        }

                            // 跳過 PES Header (通常是 9 Bytes + 附加長度)
                            int pes_header_len = 9 + payload[8];
                            if (payload_len > pes_header_len)
                            {
                                payload += pes_header_len;
                                payload_len -= pes_header_len;

                                // 準備新的緩衝區
                                if (!has_active_buffer)
                                {
                                    if (xQueueReceive(free_queue, &current_rx_frame, 0) == pdTRUE)
                                    {
                                        has_active_buffer = true;
                                    }
                                }

                                // 如果成功拿到緩衝區，才開始接收新幀
                                if (has_active_buffer)
                                {
                                    current_frame_len = 0;
                                    frame_corrupted = false;
                                    last_cc = cc;
                                }
                                else
                                {
                                    frame_corrupted = true;
                                }
                            }
                            else
                            {
                                continue;
                            }
                        }
                    }

                    // 處理鎖定的視訊流資料
                    if (pid == video_pid && has_active_buffer)
                    {
                        // 檢查丟包 (連續性計數器)
                        if (!pusi)
                        {
                            int expected_cc = (last_cc + 1) & 0x0F;
                            if (cc != expected_cc)
                            {
                                ESP_LOGW("TS", "Packet drop detected! Expected CC %d, got %d. Dropping frame.", expected_cc, cc);
                                g_video_packet_drop_count++; // 累加视频丢包计数
                                frame_corrupted = true; // 發生丟包，標記此幀為損壞，硬體安全了！
                            }
                            last_cc = cc;
                        }

                        // 如果這幀完美無缺，就將數據拷貝到 PSRAM
                        if (!frame_corrupted)
                        {
                            if (current_frame_len + payload_len <= FRAME_BUF_SIZE)
                            {
                                memcpy(current_rx_frame.buf_ptr + current_frame_len, payload, payload_len);
                                current_frame_len += payload_len;

                                // 快速偵測 JPEG 結尾 (FF D9)，一旦出現立刻送入解碼器，降低延遲
                                if (current_frame_len >= 2 &&
                                    current_rx_frame.buf_ptr[current_frame_len - 2] == 0xFF &&
                                    current_rx_frame.buf_ptr[current_frame_len - 1] == 0xD9)
                                {

                                    current_rx_frame.len = current_frame_len;
                                    if (xQueueSend(jpeg_queue, &current_rx_frame, 0) == pdTRUE)
                                    {
                                        has_active_buffer = false;
                                    }
                                    else
                                    {
                                        // 队列满，丢弃这帧的数据，复位指针等待下一帧 PUSI
                                        current_frame_len = 0;
                                    }
                                    // 幀已經送出，等待下一個 PUSI 到來
                                    frame_corrupted = true;
                                }
                            }
                            else
                            {
                                ESP_LOGW("TS", "Frame overflow > %d Bytes!", FRAME_BUF_SIZE);
                                frame_corrupted = true; // 超出緩衝區，放棄此幀
                            }
                        }
                    }
                }
            }
            else if (errno != EAGAIN && errno != EWOULDBLOCK)
            {
                ESP_LOGE("UDP", "Socket error: %d", errno);
                vTaskDelay(pdMS_TO_TICKS(100));
                break; // 修复：必须跳出内层循环，以便正确 close(sock) 重建套接字
            }
        }

        if (has_active_buffer)
        {
            xQueueSend(free_queue, &current_rx_frame, 0);
            has_active_buffer = false;
        }
        close(sock);
    }
    free(read_buf);
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
        char user_enc[64], pwd_enc[128];
        if (httpd_query_key_value(buf, "user", user_enc, sizeof(user_enc)) == ESP_OK && 
            httpd_query_key_value(buf, "pwd", pwd_enc, sizeof(pwd_enc)) == ESP_OK)
        {
            char user[32], pwd[64];
            url_decode_safe(user, user_enc, sizeof(user));
            url_decode_safe(pwd, pwd_enc, sizeof(pwd));

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
    httpd_resp_send(req, NULL, 0);
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
    httpd_resp_send(req, "{\"success\":true,\"message\":\"device will restart...\"}", HTTPD_RESP_USE_STRLEN);
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
        esp_ota_abort(update_handle);
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
    httpd_resp_send(req, "{\"success\":true,\"message\":\"update successful, device will restart...\"}", HTTPD_RESP_USE_STRLEN);
    vTaskDelay(pdMS_TO_TICKS(1000));
    esp_restart();
    return ESP_OK;
}

static esp_err_t api_version_handler(httpd_req_t *req)
{
    char json_resp[128];
    snprintf(json_resp, sizeof(json_resp), "{\"fw_ver\":\"%s\"}", PROJECT_VER);
    httpd_resp_set_type(req, "application/json");
    httpd_resp_send(req, json_resp, HTTPD_RESP_USE_STRLEN);
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
    char json_resp[1400]; // 扩展容量以容纳文件位置和网络丢包信息
    uint64_t sd_total = 0, sd_free = 0;
    if (g_sd_card_mounted)
    {
        esp_vfs_fat_info(MOUNT_POINT, &sd_total, &sd_free);
    }

    snprintf(json_resp, sizeof(json_resp),
             "{\"ssid\":\"%s\",\"rssi\":%d,\"fps\":%d,\"brightness\":%ld,\"volume\":%ld,\"osd\":%d,\"time_osd\":%d,\"user\":\"%s\",\"ip\":\"%s\",\"ip6\":\"%s\",\"timezone\":\"%s\",\"udp_port\":%ld,\"rgb_r\":%d,\"rgb_g\":%d,\"rgb_b\":%d,\"sd_mounted\":%d,\"sd_playing\":%d,\"sd_total_mb\":%lu,\"sd_free_mb\":%lu,\"audio\":%d,\"bat_mv\":%d,\"bat_pct\":%d,\"sd_pos\":%lu,\"sd_size\":%lu,\"sd_file\":\"%s\",\"fw_ver\":\"%s\",\"audio_plc\":%lu,\"video_drop\":%lu}",
             current_ssid, g_current_rssi, g_current_fps, g_current_brightness, g_current_volume, g_show_osd, g_show_time_osd, g_admin_user, g_device_ip, g_device_ip6, g_timezone, g_udp_port, g_rgb_r, g_rgb_g, g_rgb_b, g_sd_card_mounted, g_is_playing_from_sd, (unsigned long)(sd_total / 1048576), (unsigned long)(sd_free / 1048576), g_audio_enable, g_battery_voltage_mv, g_battery_percentage, (unsigned long)g_sd_current_pos, (unsigned long)g_sd_file_size, s_playback_filename, PROJECT_VER, (unsigned long)g_audio_plc_count, (unsigned long)g_video_packet_drop_count);
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
        char save_str[8] = {0};
        int save = 1; // 默认保存，兼容老调用
        if (httpd_query_key_value(buf, "save", save_str, sizeof(save_str)) == ESP_OK)
        {
            save = atoi(save_str);
        }

        if (httpd_query_key_value(buf, "brightness", param, sizeof(param)) == ESP_OK)
        {
            int32_t br = atoi(param);
            if (br < 0)
                br = 0;
            if (br > 100)
                br = 100;
            g_current_brightness = br;
            if (save)
            {
                save_int_to_nvs("brightness", br);
            }
            ledc_set_duty(LEDC_LOW_SPEED_MODE, LEDC_CHANNEL_0, (br * 255) / 100);
            ledc_update_duty(LEDC_LOW_SPEED_MODE, LEDC_CHANNEL_0);
        }
        if (httpd_query_key_value(buf, "volume", param, sizeof(param)) == ESP_OK)
        {
            int32_t vol = atoi(param);
            if (vol < 0)
                vol = 0;
            if (vol > 100)
                vol = 100;
            g_current_volume = vol;
            if (save)
            {
                save_int_to_nvs("volume", vol);
            }
            uint8_t vol_reg = vol == 0 ? 0 : ((vol * 256 / 100) - 1);
            es8311_write_reg(0x32, vol_reg);
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
        if (httpd_query_key_value(buf, "audio", param, sizeof(param)) == ESP_OK)
        {
            g_audio_enable = (atoi(param) == 1) ? 1 : 0;
            if (save)
            {
                save_i8_to_nvs("audio_enable", g_audio_enable);
            }
            gpio_set_level(PA_EN_PIN, g_audio_enable ? 0 : 1);
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

static esp_err_t api_rgb_handler(httpd_req_t *req)
{
    if (!is_authenticated(req))
    {
        httpd_resp_set_status(req, "401 Unauthorized");
        httpd_resp_send(req, "Unauthorized", HTTPD_RESP_USE_STRLEN);
        return ESP_OK;
    }

    char buf[128];
    if (httpd_req_get_url_query_str(req, buf, sizeof(buf)) == ESP_OK)
    {
        char r_str[8] = {0}, g_str[8] = {0}, b_str[8] = {0}, save_str[8] = {0};
        if (httpd_query_key_value(buf, "r", r_str, sizeof(r_str)) == ESP_OK &&
            httpd_query_key_value(buf, "g", g_str, sizeof(g_str)) == ESP_OK &&
            httpd_query_key_value(buf, "b", b_str, sizeof(b_str)) == ESP_OK)
        {
            int save = 1; // 默认保存，兼容老调用
            if (httpd_query_key_value(buf, "save", save_str, sizeof(save_str)) == ESP_OK)
            {
                save = atoi(save_str);
            }

            if (s_led_strip)
            {
                g_rgb_r = atoi(r_str);
                g_rgb_g = atoi(g_str);
                g_rgb_b = atoi(b_str);
                led_strip_set_pixel(s_led_strip, 0, g_rgb_r, g_rgb_g, g_rgb_b);
                led_strip_refresh(s_led_strip);

                if (save)
                {
                    save_u8_to_nvs("rgb_r", g_rgb_r);
                    save_u8_to_nvs("rgb_g", g_rgb_g);
                    save_u8_to_nvs("rgb_b", g_rgb_b);
                }
            }
        }
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

static esp_err_t api_sd_format_handler(httpd_req_t *req)
{
    if (!is_authenticated(req))
    {
        httpd_resp_set_status(req, "401 Unauthorized");
        httpd_resp_send(req, "Unauthorized", HTTPD_RESP_USE_STRLEN);
        return ESP_OK;
    }
    if (!g_sd_card_mounted)
    {
        httpd_resp_set_status(req, "400 Bad Request");
        httpd_resp_send(req, "SD Card not mounted", HTTPD_RESP_USE_STRLEN);
        return ESP_OK;
    }

    if (esp_vfs_fat_sdcard_format(MOUNT_POINT, s_card) == ESP_OK)
    {
        httpd_resp_send(req, "OK", HTTPD_RESP_USE_STRLEN);
    }
    else
    {
        ESP_LOGE("SD", "Format failed, unmounting card.");
        esp_vfs_fat_sdcard_unmount(MOUNT_POINT, s_card);
        g_sd_card_mounted = false;
        httpd_resp_set_status(req, "500 Internal Server Error");
        httpd_resp_send(req, "Format failed", HTTPD_RESP_USE_STRLEN);
    }
    return ESP_OK;
}

static esp_err_t api_sd_play_handler(httpd_req_t *req)
{
    if (!is_authenticated(req))
    {
        httpd_resp_set_status(req, "401 Unauthorized");
        return httpd_resp_send(req, "Unauthorized", HTTPD_RESP_USE_STRLEN);
    }
    if (g_is_playing_from_sd)
    {
        httpd_resp_set_status(req, "409 Conflict");
        return httpd_resp_send(req, "Already playing a file.", HTTPD_RESP_USE_STRLEN);
    }

    char buf[200];
    if (httpd_req_get_url_query_str(req, buf, sizeof(buf)) == ESP_OK)
    {
        char file_param[128];
        if (httpd_query_key_value(buf, "file", file_param, sizeof(file_param)) == ESP_OK)
        {
            url_decode_safe(s_playback_filename, file_param, sizeof(s_playback_filename));
            if (strchr(s_playback_filename, '/') != NULL || strchr(s_playback_filename, '\\') != NULL)
            {
                httpd_resp_set_status(req, "400 Bad Request");
                return httpd_resp_send(req, "Invalid filename", HTTPD_RESP_USE_STRLEN);
            }
            xTaskNotifyGive(s_sd_playback_task_handle);
            return httpd_resp_send(req, "OK", HTTPD_RESP_USE_STRLEN);
        }
    }
    httpd_resp_set_status(req, "400 Bad Request");
    return httpd_resp_send(req, "File parameter missing", HTTPD_RESP_USE_STRLEN);
}

static esp_err_t api_sd_stop_handler(httpd_req_t *req)
{
    if (!is_authenticated(req))
    {
        httpd_resp_set_status(req, "401 Unauthorized");
        return httpd_resp_send(req, "Unauthorized", HTTPD_RESP_USE_STRLEN);
    }
    if (g_is_playing_from_sd)
    {
        g_stop_playback = true;
        // Wait a moment for the task to acknowledge the stop signal
        for (int i = 0; i < 10 && g_is_playing_from_sd; i++)
        {
            vTaskDelay(pdMS_TO_TICKS(100));
        }
    }
    httpd_resp_send(req, "OK", HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}

static esp_err_t api_sd_pause_handler(httpd_req_t *req)
{
    if (!is_authenticated(req))
    {
        httpd_resp_set_status(req, "401 Unauthorized");
        return httpd_resp_send(req, "Unauthorized", HTTPD_RESP_USE_STRLEN);
    }
    if (g_is_playing_from_sd)
    {
        g_pause_playback = true;
    }
    httpd_resp_send(req, "OK", HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}

static esp_err_t api_sd_resume_handler(httpd_req_t *req)
{
    if (!is_authenticated(req))
    {
        httpd_resp_set_status(req, "401 Unauthorized");
        return httpd_resp_send(req, "Unauthorized", HTTPD_RESP_USE_STRLEN);
    }
    if (g_is_playing_from_sd)
    {
        g_pause_playback = false;
    }
    httpd_resp_send(req, "OK", HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}

static esp_err_t api_sd_seek_handler(httpd_req_t *req)
{
    if (!is_authenticated(req))
    {
        httpd_resp_set_status(req, "401 Unauthorized");
        return httpd_resp_send(req, "Unauthorized", HTTPD_RESP_USE_STRLEN);
    }
    if (!g_is_playing_from_sd)
    {
        return httpd_resp_send(req, "OK", HTTPD_RESP_USE_STRLEN);
    }
    char buf[64];
    if (httpd_req_get_url_query_str(req, buf, sizeof(buf)) == ESP_OK)
    {
        char val_str[16];
        if (httpd_query_key_value(buf, "val", val_str, sizeof(val_str)) == ESP_OK)
        {
            int val = atoi(val_str);
            if (val < 0)
                val = 0;
            if (val > 1000)
                val = 1000;
            g_seek_permille = val;
        }
    }
    return httpd_resp_send(req, "OK", HTTPD_RESP_USE_STRLEN);
}

static esp_err_t api_sd_delete_handler(httpd_req_t *req)
{
    if (!is_authenticated(req))
    {
        httpd_resp_set_status(req, "401 Unauthorized");
        httpd_resp_send(req, "Unauthorized", HTTPD_RESP_USE_STRLEN);
        return ESP_OK;
    }
    if (!g_sd_card_mounted)
    {
        httpd_resp_set_status(req, "400 Bad Request");
        httpd_resp_send(req, "SD Card not mounted", HTTPD_RESP_USE_STRLEN);
        return ESP_OK;
    }

    char buf[128];
    if (httpd_req_get_url_query_str(req, buf, sizeof(buf)) == ESP_OK)
    {
        char file_param[64];
        if (httpd_query_key_value(buf, "file", file_param, sizeof(file_param)) == ESP_OK)
        {
            char decoded_file[64];
            url_decode_safe(decoded_file, file_param, sizeof(decoded_file));

            // 防止跨目录攻击
            if (strchr(decoded_file, '/') != NULL || strchr(decoded_file, '\\') != NULL)
            {
                httpd_resp_set_status(req, "400 Bad Request");
                httpd_resp_send(req, "Invalid filename", HTTPD_RESP_USE_STRLEN);
                return ESP_OK;
            }

            char filepath[128];
            snprintf(filepath, sizeof(filepath), "%s/%s", MOUNT_POINT, decoded_file);

            if (unlink(filepath) == 0)
            {
                return httpd_resp_send(req, "OK", HTTPD_RESP_USE_STRLEN);
            }
            else
            {
                ESP_LOGE("SD", "Failed to delete file %s", filepath);
                // Check if the card is still there.
                uint64_t total, free;
                if (esp_vfs_fat_info(MOUNT_POINT, &total, &free) != ESP_OK)
                {
                    ESP_LOGE("SD", "Card inaccessible after delete failure. Unmounting.");
                    esp_vfs_fat_sdcard_unmount(MOUNT_POINT, s_card);
                    g_sd_card_mounted = false;
                }
            }
        }
    }

    httpd_resp_set_status(req, "500 Internal Server Error");
    httpd_resp_send(req, "Delete failed", HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}

static esp_err_t api_sd_eject_handler(httpd_req_t *req)
{
    if (!is_authenticated(req))
    {
        httpd_resp_set_status(req, "401 Unauthorized");
        httpd_resp_send(req, "Unauthorized", HTTPD_RESP_USE_STRLEN);
        return ESP_OK;
    }
    if (!g_sd_card_mounted)
    {
        httpd_resp_set_status(req, "400 Bad Request");
        httpd_resp_send(req, "SD Card not mounted", HTTPD_RESP_USE_STRLEN);
        return ESP_OK;
    }

    // If playing, stop it first
    if (g_is_playing_from_sd)
        g_stop_playback = true;

    if (esp_vfs_fat_sdcard_unmount(MOUNT_POINT, s_card) == ESP_OK)
    {
        g_sd_card_mounted = false;
        return httpd_resp_send(req, "OK", HTTPD_RESP_USE_STRLEN);
    }

    httpd_resp_set_status(req, "500 Internal Server Error");
    return httpd_resp_send(req, "Eject failed", HTTPD_RESP_USE_STRLEN);
}

static esp_err_t api_files_handler(httpd_req_t *req)
{
    if (!is_authenticated(req))
    {
        httpd_resp_set_status(req, "401 Unauthorized");
        httpd_resp_send(req, "Unauthorized", HTTPD_RESP_USE_STRLEN);
        return ESP_OK;
    }
    // Try to ensure the card is mounted before proceeding.
    sd_card_init();

    httpd_resp_set_type(req, "application/json");
    httpd_resp_send_chunk(req, "[", 1);

    DIR *dir = opendir(MOUNT_POINT);
    if (dir)
    {
        struct dirent *entry;
        bool first = true;
        char buf[512];
        while ((entry = readdir(dir)) != NULL)
        {
            if (entry->d_type == DT_REG)
            {
                char filepath[300];
                snprintf(filepath, sizeof(filepath), "%s/%s", MOUNT_POINT, entry->d_name);
                struct stat st;
                if (stat(filepath, &st) == 0)
                {
                    snprintf(buf, sizeof(buf), "%s{\"name\":\"%s\",\"size\":%ld}",
                             first ? "" : ",", entry->d_name, (long)st.st_size);
                    httpd_resp_send_chunk(req, buf, strlen(buf));
                    first = false;
                }
            }
        }
        closedir(dir);
    }
    httpd_resp_send_chunk(req, "]", 1);
    httpd_resp_send_chunk(req, NULL, 0);
    return ESP_OK;
}

static esp_err_t api_sd_upload_handler(httpd_req_t *req)
{
    if (!is_authenticated(req))
    {
        httpd_resp_set_status(req, "401 Unauthorized");
        httpd_resp_send(req, "Unauthorized", HTTPD_RESP_USE_STRLEN);
        return ESP_FAIL;
    }
    if (!g_sd_card_mounted)
    {
        httpd_resp_set_status(req, "400 Bad Request");
        httpd_resp_send(req, "SD Card not mounted", HTTPD_RESP_USE_STRLEN);
        return ESP_FAIL;
    }

    char buf[128];
    char file_param[64] = {0};
    if (httpd_req_get_url_query_str(req, buf, sizeof(buf)) == ESP_OK)
    {
        httpd_query_key_value(buf, "file", file_param, sizeof(file_param));
    }

    if (strlen(file_param) == 0)
    {
        httpd_resp_set_status(req, "400 Bad Request");
        httpd_resp_send(req, "Filename missing", HTTPD_RESP_USE_STRLEN);
        return ESP_FAIL;
    }

    char decoded_file[64];
    url_decode_safe(decoded_file, file_param, sizeof(decoded_file));
    if (strchr(decoded_file, '/') != NULL || strchr(decoded_file, '\\') != NULL)
    {
        httpd_resp_set_status(req, "400 Bad Request");
        httpd_resp_send(req, "Invalid filename", HTTPD_RESP_USE_STRLEN);
        return ESP_FAIL;
    }

    char filepath[128];
    snprintf(filepath, sizeof(filepath), "%s/%s", MOUNT_POINT, decoded_file);

    FILE *f = fopen(filepath, "wb");
    if (!f)
    {
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }

    char *data_buf = malloc(4096);
    if (!data_buf)
    {
        fclose(f);
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }

    int remaining = req->content_len;
    esp_err_t err = ESP_OK;

    while (remaining > 0)
    {
        int received = httpd_req_recv(req, data_buf, MIN(remaining, 4096));
        if (received < 0)
        {
            if (received == HTTPD_SOCK_ERR_TIMEOUT)
            {
                // 超时重试
                continue;
            }
            err = ESP_FAIL;
            break;
        }
        else if (received == 0)
        {
            err = ESP_FAIL;
            break;
        }
        if (fwrite(data_buf, 1, received, f) != received)
        {
            err = ESP_FAIL;
            break;
        }
        remaining -= received;

        // 让出 CPU
        vTaskDelay(pdMS_TO_TICKS(1));
    }

    free(data_buf);
    fclose(f);

    if (err != ESP_OK)
    {
        unlink(filepath); // 删除不完整的文件
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }

    httpd_resp_send(req, "OK", HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}

static esp_err_t sd_file_download_handler(httpd_req_t *req)
{
    if (!is_authenticated(req))
    {
        httpd_resp_set_status(req, "401 Unauthorized");
        httpd_resp_send(req, "Unauthorized", HTTPD_RESP_USE_STRLEN);
        return ESP_OK;
    }
    char filepath[128];
    strlcpy(filepath, req->uri, sizeof(filepath));

    // 移除 URL 可能附带的 Query Params
    char *q = strchr(filepath, '?');
    if (q)
        *q = '\0';

    FILE *f = fopen(filepath, "r");
    if (!f)
        return httpd_resp_send_404(req);

    httpd_resp_set_type(req, "video/x-motion-jpeg");

    char *chunk = malloc(8192);
    if (!chunk)
    {
        fclose(f);
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }
    size_t read_bytes;
    while ((read_bytes = fread(chunk, 1, 8192, f)) > 0)
    {
        if (httpd_resp_send_chunk(req, chunk, read_bytes) != ESP_OK)
        {
            fclose(f);
            free(chunk);
            return ESP_FAIL;
        }
    }
    fclose(f);
    free(chunk);
    httpd_resp_send_chunk(req, NULL, 0);
    return ESP_OK;
}

static httpd_handle_t start_webserver(void)
{
    httpd_config_t config = HTTPD_DEFAULT_CONFIG();
    config.server_port = 80;
    config.max_uri_handlers = 26;                   // 增加处理路由数以支持 seek 接口
    config.max_open_sockets = 3;                    // 限制普通接口服务器的并发连接数
    config.lru_purge_enable = true;                 // 自动踢掉老旧闲置连接，保护可用 Socket
    config.uri_match_fn = httpd_uri_match_wildcard; // 开启通配符路由匹配
    httpd_handle_t server = NULL;
    if (httpd_start(&server, &config) == ESP_OK)
    {
        httpd_uri_t index_uri = {.uri = "/", .method = HTTP_GET, .handler = index_get_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &index_uri);
        httpd_uri_t login_uri = {.uri = "/login", .method = HTTP_GET, .handler = login_get_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &login_uri);
        httpd_uri_t do_login_uri = {.uri = "/do_login", .method = HTTP_GET, .handler = do_login_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &do_login_uri);
        httpd_uri_t api_version_uri = {.uri = "/api/version", .method = HTTP_GET, .handler = api_version_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &api_version_uri);
        httpd_uri_t api_data_uri = {.uri = "/api/data", .method = HTTP_GET, .handler = api_data_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &api_data_uri);
        httpd_uri_t api_set_uri = {.uri = "/api/set", .method = HTTP_GET, .handler = api_set_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &api_set_uri);
        httpd_uri_t api_rgb_uri = {.uri = "/api/rgb", .method = HTTP_GET, .handler = api_rgb_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &api_rgb_uri);
        httpd_uri_t api_acc_uri = {.uri = "/api/account", .method = HTTP_GET, .handler = api_account_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &api_acc_uri);
        httpd_uri_t api_wifi_reset_uri = {.uri = "/api/wifi_reset", .method = HTTP_POST, .handler = api_wifi_reset_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &api_wifi_reset_uri);
        httpd_uri_t ota_uri = {.uri = "/api/ota", .method = HTTP_POST, .handler = api_ota_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &ota_uri);
        httpd_uri_t sd_format_uri = {.uri = "/api/sd_format", .method = HTTP_POST, .handler = api_sd_format_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &sd_format_uri);
        httpd_uri_t do_restart_uri = {.uri = "/api/restart", .method = HTTP_POST, .handler = do_restart_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &do_restart_uri);
        httpd_uri_t api_files_uri = {.uri = "/api/files", .method = HTTP_GET, .handler = api_files_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &api_files_uri);
        httpd_uri_t api_sd_delete_uri = {.uri = "/api/sd_delete", .method = HTTP_POST, .handler = api_sd_delete_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &api_sd_delete_uri);
        httpd_uri_t sd_eject_uri = {.uri = "/api/sd_eject", .method = HTTP_POST, .handler = api_sd_eject_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &sd_eject_uri);
        httpd_uri_t sd_play_uri = {.uri = "/api/sd_play", .method = HTTP_POST, .handler = api_sd_play_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &sd_play_uri);
        httpd_uri_t sd_stop_uri = {.uri = "/api/sd_stop", .method = HTTP_POST, .handler = api_sd_stop_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &sd_stop_uri);
        httpd_uri_t sd_pause_uri = {.uri = "/api/sd_pause", .method = HTTP_POST, .handler = api_sd_pause_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &sd_pause_uri);
        httpd_uri_t sd_resume_uri = {.uri = "/api/sd_resume", .method = HTTP_POST, .handler = api_sd_resume_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &sd_resume_uri);
        httpd_uri_t sd_seek_uri = {.uri = "/api/sd_seek", .method = HTTP_POST, .handler = api_sd_seek_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &sd_seek_uri);
        httpd_uri_t api_sd_upload_uri = {.uri = "/api/sd_upload", .method = HTTP_POST, .handler = api_sd_upload_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &api_sd_upload_uri);
        httpd_uri_t sd_file_uri = {.uri = "/sdcard/*", .method = HTTP_GET, .handler = sd_file_download_handler, .user_ctx = NULL};
        httpd_register_uri_handler(server, &sd_file_uri);
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
    sd_card_init();
    audio_hardware_init();
    battery_adc_init();
    rgb_led_init();
    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));

    network_prov_mgr_config_t config_prov = {.scheme = network_prov_scheme_ble, .scheme_event_handler = NETWORK_PROV_EVENT_HANDLER_NONE};
    ESP_ERROR_CHECK(network_prov_mgr_init(config_prov));

    bool provisioned = false;
    ESP_ERROR_CHECK(network_prov_mgr_is_wifi_provisioned(&provisioned));

    xTaskCreatePinnedToCore(jpeg_decode_display_task, "jpeg_task", 8192, NULL, 10, NULL, 1);
    xTaskCreatePinnedToCore(sd_playback_task, "sd_playback", 8192, NULL, 8, &s_sd_playback_task_handle, 0);

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

    xTaskCreatePinnedToCore(audio_receiver_task, "audio_task", 4096, NULL, 12, NULL, 0); // 音频极其敏感，给予最高优先级 (12)
    xTaskCreatePinnedToCore(video_receiver_task, "video_task", 8192, NULL, 10, NULL, 0); // 视频接收其次 (10)
    start_webserver();

    TickType_t last_scan_time = 0;
    while (1)
    {
        vTaskDelay(pdMS_TO_TICKS(1000));

        // 5s 扫描一次 WiFi 信号强度和剩余内存，并根据情况触发 WiFi 扫描以更新附近 AP 列表
        if (xTaskGetTickCount() - last_scan_time >= pdMS_TO_TICKS(5000))
        {
            g_async_free_heap_kb = esp_get_free_heap_size() / 1024;
            wifi_ap_record_t ap_info;
            if (esp_wifi_sta_get_ap_info(&ap_info) == ESP_OK)
            {
                g_async_wifi_rssi = ap_info.rssi;
                snprintf((char *)g_async_wifi_bssid, sizeof(g_async_wifi_bssid), "%02X:%02X:%02X:%02X:%02X:%02X",
                         ap_info.bssid[0], ap_info.bssid[1], ap_info.bssid[2], ap_info.bssid[3], ap_info.bssid[4], ap_info.bssid[5]);

                TickType_t now_tick = xTaskGetTickCount();
                // 仅在网络空闲 (接收速度 < 50KB/s) 时才允许自动漫游扫描，绝对避免打断正在进行的视频流
                if (ap_info.rssi < -75 && now_tick - last_scan_time >= pdMS_TO_TICKS(30000) && g_current_rx_speed_kbps < 50)
                {
                    wifi_scan_config_t scan_config = {0};
                    scan_config.ssid = ap_info.ssid;
                    if (esp_wifi_scan_start(&scan_config, false) == ESP_OK)
                        last_scan_time = now_tick;
                }
            }
            else
            {
                g_async_wifi_rssi = 0;
                g_async_wifi_bssid[0] = '\0';
            }
        }

        // 3s扫描一次电池电压
        static TickType_t last_battery_scan_time = 0;
        if (adc1_handle && xTaskGetTickCount() - last_battery_scan_time >= pdMS_TO_TICKS(3000))
        {
            last_battery_scan_time = xTaskGetTickCount();
            int adc_raw = 0;
            if (adc_oneshot_read(adc1_handle, ADC_CHANNEL_8, &adc_raw) == ESP_OK)
            {
                int voltage_mv = 0;
                if (do_calibration)
                {
                    adc_cali_raw_to_voltage(adc1_cali_handle, adc_raw, &voltage_mv);
                }
                else
                {
                    voltage_mv = adc_raw;
                }
                int real_mv = voltage_mv * 2;
                g_battery_voltage_mv = real_mv;
                int pct = (real_mv - 3300) * 100 / (4200 - 3300);
                if (pct > 100)
                    pct = 100;
                if (pct < 0)
                    pct = 0;
                g_battery_percentage = pct;
            }
        }
        g_current_rx_speed_kbps = g_rx_bytes_1s / 1024;
        g_rx_bytes_1s = 0;
    }
}