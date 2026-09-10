/* EspDeviceInfo.cpp */
#include "ESP_Hive.h"
#include <esp_mac.h>


const table_entry MAC_table[] = {
  {"78:21:84:79:EF:88", "192.0.0.1",2}
};

const int num_ESPs = sizeof(MAC_table) / sizeof(MAC_table[0]);

//Consulta a tabela MAC, e retorna o ID da ESP32
int my_rank() {
  uint8_t mac[6];
  
  //Lê o MAC
  esp_read_mac(mac, ESP_MAC_WIFI_STA); 

  char mac_str[18];
  snprintf(mac_str, sizeof(mac_str), "%02X:%02X:%02X:%02X:%02X:%02X",
           mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
           
  //Converte para o tipo String do Arduino.h 
  String mac_string = String(mac_str);
  for (int i = 0; i < num_ESPs; i++) {
    if (mac_string.equalsIgnoreCase(MAC_table[i].MAC_address)) 
    {
      return MAC_table[i].ID; // Encontrou
    }
  }
  return -1; // Retorna código de erro se não achou

}

//Retorna o MAC da ESP32. Útil para preencher a tabela de MAC
String my_MAC(){
  uint8_t mac[6];
  esp_read_mac(mac, ESP_MAC_WIFI_STA); 

  char mac_str[18];
  snprintf(mac_str, sizeof(mac_str), "%02X:%02X:%02X:%02X:%02X:%02X",
           mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
           
  //Converte para o tipo String do Arduino.h 
  String mac_string = String(mac_str);

  return mac_str;
}

//Retorna o IP associado a um ID
String get_IP(int ID_ESP){

  for (int i = 0; i < num_ESPs; i++)
  {
    if (ID_ESP == MAC_table[i].ID)
    {
      return String(MAC_table[i].IP);
    }
  }

  return String("IP nao encontrado");
}
