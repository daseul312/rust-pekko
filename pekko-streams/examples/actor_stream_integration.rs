//! Actor-Stream Integration Example
//!
//! This example demonstrates how to connect actors with streams for
//! real-time data processing with backpressure.

use pekko_streams::prelude::*;
use tokio::sync::mpsc;
use std::time::Duration;

/// Simulates a data producer (like an actor receiving market data)
async fn data_producer(sender: mpsc::Sender<f64>) {
    let prices = vec![100.0, 101.5, 99.8, 102.3, 103.1, 98.5, 104.0, 105.2];
    
    for price in prices {
        println!("[Producer] Sending price: {:.2}", price);
        
        // Backpressure: if the stream is slow, this will wait
        if sender.send(price).await.is_err() {
            println!("[Producer] Stream closed");
            break;
        }
        
        // Simulate some delay between data points
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    
    println!("[Producer] Done producing");
}

/// Simulates a data consumer (like an actor processing results)
async fn data_consumer(mut receiver: mpsc::Receiver<String>) {
    println!("[Consumer] Waiting for processed data...");
    
    while let Some(alert) = receiver.recv().await {
        println!("[Consumer] Received: {}", alert);
    }
    
    println!("[Consumer] Stream complete");
}

#[tokio::main]
async fn main() {
    println!("=== Actor-Stream Integration Demo ===\n");

    // 1. Create a source that actors can send to
    let (source, producer_sender) = ActorSourceBuilder::new(10).build::<f64>();
    
    // 2. Create a sink that sends to actors
    let (sink, consumer_receiver) = ActorSinkBuilder::new(10).build::<String>();

    // 3. Start the producer "actor" (simulated)
    let producer_handle = tokio::spawn(data_producer(producer_sender));

    // 4. Start the consumer "actor" (simulated)
    let consumer_handle = tokio::spawn(data_consumer(consumer_receiver));

    // 5. Run the stream pipeline
    let stream_handle = tokio::spawn(async move {
        source
            // Calculate price change
            .map(|price| {
                println!("[Stream] Processing price: {:.2}", price);
                price
            })
            // Filter significant moves (> $100)
            .filter(|&price| price > 100.0)
            // Generate alert message
            .map(|price| format!("🔔 Price Alert: ${:.2}", price))
            // Send to consumer
            .run_with(sink)
            .await
            .unwrap();
    });

    // Wait for all tasks
    let _ = tokio::join!(producer_handle, stream_handle, consumer_handle);

    println!("\n=== Demo Complete ===");

    // =========================================
    // Example 2: Using SourceQueue for more control
    // =========================================
    println!("\n=== SourceQueue Example ===\n");

    let (source, queue) = source_queue::<i32>(5);
    
    // Try to offer elements
    println!("Offering elements to queue...");
    assert!(queue.try_offer(1).unwrap());
    assert!(queue.try_offer(2).unwrap());
    assert!(queue.try_offer(3).unwrap());
    
    // Complete the queue
    queue.complete();
    
    // Process the stream
    let result = source
        .map(|x| x * 10)
        .run_collect()
        .await
        .unwrap();
    
    println!("Result: {:?}", result);
    assert_eq!(result, vec![10, 20, 30]);

    println!("\n=== All Examples Complete! ===");
}
