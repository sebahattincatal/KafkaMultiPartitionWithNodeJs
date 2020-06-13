const { Kafka } = require("kafkajs");

const topic_name = process.argv[2] || "Logs2";

createConsumer();

async function createConsumer() {
    try {
        
        const kafka = new Kafka({
            clientId : "kafka_log_store_client",
            brokers : ["127.0.0.1:9092"]
        });
    
        const consumer = kafka.consumer({
            groupId : "log_Store_consumer_group"
        });

        console.log("consumer'a bağlanılıyor.");
        await consumer.connect();
        console.log("consumer'a bağlantı başarılı.");
    
        // Consumer Subscribe ..
        await consumer.subscribe({
            topic : "LogStoreTopic",
            fromBeginning : true // Başlangıçtan başla
        });

        await consumer.run({
            eachMessage : async result => {
                console.log(`Gelen mesaj ${result.message.value} : Partition : => ${result.partition}`);
            }
        });

    } catch (error) {
        console.log("Bir hata oluştu...", error);
    }
}