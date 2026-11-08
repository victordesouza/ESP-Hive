/*
    Funções para a comunicação do cluster
*/

#ifndef ESP_Hive_Comm
#define ESP_Hive_Comm

void hive_init();
void hive_send();
void hive_recv();

void hive_send_handler();
void hive_recv_hander();


#endif

