#include <WiFi.h>
#include "ESP_Hive.h" 

const char* ssid = "ESP_HIVE_NET"; 
const char* password = "hive_password";

// Configurações de IP para a Queen (Master)
IPAddress queen_ip(192, 168, 50, 1);
IPAddress gateway(192, 168, 50, 1);
IPAddress subnet(255, 255, 255, 0);

// Configuração do Servidor TCP
const int hive_port = 8080;
WiFiServer queen_server(hive_port);
WiFiClient worker_client; // Usado pela Operária para se conectar

void setup() {
  Serial.begin(115200);
  delay(1000);
  Serial.println();

  int my_id = my_rank(); 

  if (my_id == 1) {
    queen_setup();
  } 
  else if (my_id > 1) {
    worker_setup(my_id);
  } 
  else {
    Serial.println("ERROR: ESP32 MAC not registered.");
    Serial.print("Current MAC: ");
    Serial.println(my_MAC()); 
  }
}

void loop() {
  int my_id = my_rank();
  
  if (my_id == 1) {
    queen_process_nectar();
  } 
  else if (my_id > 1) {
    worker_send_nectar(my_id);
  }
}

// ==========================================
// QUEEN (TCP SERVER) FUNCTIONS
// ==========================================

void queen_setup() {
  Serial.println("=== HIVE QUEEN (TCP SERVER) ===");
  
  if (!WiFi.softAPConfig(queen_ip, gateway, subnet)) {
    Serial.println("Error configuring AP IP");
    return;
  }
  
  if (!WiFi.softAP(ssid, password)) {
    Serial.println("Error creating Access Point");
    return;
  }
  
  // Inicia o servidor TCP na porta especificada
  queen_server.begin();
  
  Serial.println("Hive network established!");
  Serial.print("Queen IP: ");
  Serial.println(WiFi.softAPIP());
  Serial.print("Listening on Port: ");
  Serial.println(hive_port);
}

void queen_process_nectar() {
  // Verifica se há algum cliente (Operária) tentando se conectar
  WiFiClient incoming_worker = queen_server.available();
  
  if (incoming_worker) {
    Serial.print("\nNew worker connected from IP: ");
    Serial.println(incoming_worker.remoteIP());
    
    // Aguarda até que a Operária envie dados ou a conexão feche
    while (incoming_worker.connected()) {
      if (incoming_worker.available()) {
        char incoming_nectar[255];
        
        // Lê os bytes recebidos via TCP
        int len = incoming_worker.readBytesUntil('\n', incoming_nectar, sizeof(incoming_nectar) - 1);
        incoming_nectar[len] = '\0'; // Garante que a string está finalizada
        
        Serial.print("Nectar received: ");
        Serial.println(incoming_nectar);
        
        // Exemplo: a Rainha pode enviar um ACK de volta
        incoming_worker.println("ACK");
        break; // Sai do loop após receber os dados
      }
    }
    // Fecha a conexão após a troca de dados (opcional, dependendo do design)
    incoming_worker.stop(); 
    Serial.println("Worker disconnected.");
  }
}

// ==========================================
// WORKER (TCP CLIENT) FUNCTIONS
// ==========================================

void worker_setup(int worker_id) {
  Serial.print("=== HIVE WORKER (ID: ");
  Serial.print(worker_id);
  Serial.println(") ===");
  
  WiFi.begin(ssid, password);
  Serial.print("Connecting to Queen AP ");
  
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  
  Serial.println("\nConnected to Hive AP!");
  Serial.print("Worker IP: ");
  Serial.println(WiFi.localIP());
}

void worker_send_nectar(int worker_id) {
  static unsigned long last_foraging_time = 0;
  const unsigned long foraging_interval = 5000; 
  
  if (millis() - last_foraging_time >= foraging_interval) {
    Serial.print("Connecting to Queen Server... ");
    
    // Tenta estabelecer a conexão TCP (3-way handshake)
    if (worker_client.connect(queen_ip, hive_port)) {
      Serial.println("Connected!");
      
      char payload_buffer[50];
      snprintf(payload_buffer, sizeof(payload_buffer), "Worker_ID_%d_reporting\n", worker_id);
      
      // Envia os dados pelo socket TCP
      worker_client.print(payload_buffer);
      Serial.print("Sent: ");
      Serial.print(payload_buffer);
      
      // (Opcional) Aguarda um ACK da Rainha
      unsigned long timeout = millis();
      while (worker_client.available() == 0) {
        if (millis() - timeout > 2000) {
          Serial.println("Timeout waiting for ACK.");
          break;
        }
      }
      
      if (worker_client.available()) {
        String ack = worker_client.readStringUntil('\n');
        Serial.print("Queen replied: ");
        Serial.println(ack);
      }
      
      // Encerra a conexão TCP ("desliga" o socket)
      worker_client.stop(); 
    } 
    else {
      Serial.println("Connection failed.");
    }
    
    last_foraging_time = millis();
  }
}