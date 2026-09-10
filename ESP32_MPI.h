/* EspDeviceInfo.h */
#ifndef ESP32_MPI
#define ESP32_MPI
#include <Arduino.h> //Para tipo String
#define TABLE_SIZE



struct EntradaTabela {
  const char* macAddress;
  const char* IP;
  int ID;
};



int myRank();
String getIP(int ID_ESP);
int hive_send(const void *buf, int count, size_t datatype, int dest, int tag);
int hive_recv(void *buf, int count, size_t datatype, int source, int tag);



#endif