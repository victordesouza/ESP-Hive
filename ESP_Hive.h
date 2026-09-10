/* EspDeviceInfo.h */
#ifndef ESP_Hive
#define ESP_Hive
#include <Arduino.h> //Para tipo String



struct table_entry {
  const char* MAC_address;
  const char* IP;
  int ID;
};



int my_rank();
String get_IP(int ID_ESP);




#endif