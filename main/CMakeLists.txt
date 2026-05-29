idf_component_register(SRCS "main.c"
                    INCLUDE_DIRS "."
                    EMBED_TXTFILES "web/index.html" "web/login.html"
                    REQUIRES 
                        esp_wifi 
                        esp_timer 
                        freertos 
                        nvs_flash 
                        esp_http_server 
                        app_update 
                        esp_https_ota 
                        network_provisioning   
                        bt
                    PRIV_REQUIRES 
                        esp_driver_ledc 
                        esp_lcd
                        esp_lcd_ili9341
)