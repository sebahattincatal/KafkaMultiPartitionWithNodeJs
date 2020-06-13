const { Kafka } = require("kafkajs");
const log_data = require("./system_logs.json");

createProcedure();

async function createProcedure() {
    try {
        
        const kafka = new Kafka({
            clientId : "kafka_log_store_client",
            brokers : ["127.0.0.1:9092"]
        });
    
        const producer = kafka.producer();
        console.log("producer'a bağlanılıyor.");
        await producer.connect();
    
        let messages = log_data.map(item => {
            return {
                value : JSON.stringify(item),
                partition : item.type == "system" ? 0 : 1 
            };
        });

        const message_result = await producer.send({
            topic : "LogStoreTopic",
            messages : messages
        });

        console.log("Gönderim işlemi başarılı", JSON.stringify(message_result));
        producer.disconnect();

    } catch (error) {
        console.log("Bir hata oluştu...", error);
    } finally {
        process.exit(0);
    }
}