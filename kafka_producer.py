import pandas as pd
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'hospital_admissions_stream'

def simulate_streaming():
    print("Bắt đầu đọc dữ liệu tĩnh và kích hoạt sensor IoT ảo...")
    df = pd.read_csv('./data/raw/diabetic_data.csv')
    
    print(f"Bắt đầu đẩy luồng dữ liệu vào topic: {TOPIC_NAME}...")
    
    for index, row in df.iterrows():
        record = row.to_dict()
        
        record['timestamp'] = datetime.now().isoformat()
        
        record['heart_rate'] = random.randint(60, 100)           
        record['blood_pressure_sys'] = random.randint(90, 120)   
        record['spo2'] = random.randint(95, 100)                 

        if random.random() < 0.05:
            record['heart_rate'] = random.randint(130, 180)      
            record['spo2'] = random.randint(85, 90)             
            
        producer.send(TOPIC_NAME, value=record)
        
        print(f"[Đã gửi] ID: {record.get('patient_nbr')} | Lúc: {record['timestamp']} | Nhịp tim: {record['heart_rate']} bpm | SpO2: {record['spo2']}%")
        
        time.sleep(1)

if __name__ == "__main__":
    try:
        simulate_streaming()
    except KeyboardInterrupt:
        print("\nĐã ngắt kết nối sensor ảo an toàn.")
    finally:
        producer.close()