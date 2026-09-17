#ifndef ESP_Hive
#define ESP_Hive
#include <Arduino.h> //Para tipo String
#include <Ethernet.h>


struct table_entry {
  const char* MAC_address;
  IPAddress IP;
  int ID;
};



int my_rank();
String my_MAC();
IPAddress get_IP(int ID_ESP);




#endif