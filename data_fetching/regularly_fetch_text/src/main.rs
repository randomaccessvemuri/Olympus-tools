use mongodb::{ 
    bson::{doc, Document}, options::ClientOptions, Client, Collection 
};

use std::{env, result};
use std::error::Error;
use futures::{stream::FuturesUnordered, StreamExt};
use tokio::time::{interval, Duration, Instant};
use std::sync::Arc;
use tokio::sync::Semaphore;
use indicatif::{ProgressBar, ProgressStyle};
use std::io::{stdout, Write};

//Using package article_scraper
async fn fetch_text(url: &str) -> Result<String, Box<dyn Error + Send + Sync>> {
    let mut result = String::new();
    let scraper = article_scraper::ArticleScraper::new(None).await;
    let url = url::Url::parse(url)?;

    let client = reqwest::Client::new();
    let article = scraper.parse(&url, false, &client, None);

    if let Ok(article) = article.await {
        let intermediate = article.html.unwrap();
        let document = scraper::Html::parse_document(&intermediate);
        let selector = scraper::Selector::parse("p").unwrap();
        for element in document.select(&selector) {
            let text = element.text().collect::<Vec<_>>().join(" ");
            result.push_str(&text);
        }
    }



    Ok(result)
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        println!("This is a tiny tool that regularly fetches text for news articles which have text under a certain length (say like 45 words). These news articles are already in MongoDB, we fetch the text and write to the document in MongoDB. This text can later be used for anything like constructing summaries, topic modeling, knowledge graph construction etc.");
        println!("Usage: text_fetch <mongodb_uri> <db_name> <collection_name> <rate_limit (per minute)> <max_concurrent_tasks> <wait_time_per_cycle (in minutes)> ");
        return;
    }

    let mongodb_uri = &args[1];
    let db_name = &args[2];
    let collection_name = &args[3];
    let rate_limit = args[4].parse::<u64>().unwrap_or(60);
    let wait_time_per_cycle = args[5].parse::<usize>().unwrap_or(25);

    let client_options = ClientOptions::parse(mongodb_uri).await.unwrap();
    let client = Client::with_options(client_options).unwrap();

    let db = client.database(db_name);
    let collection: Collection<Document> = db.collection(collection_name);

    loop {
        let start_time = Instant::now();
        let mut tasks = FuturesUnordered::new();
        let mut cursor = collection.find(doc! { "content.text": { "$exists": false } }).await.unwrap();
        
        // Count the number of documents to process
        let total_docs = collection.count_documents(doc! { "content.text": { "$exists": false } }).await.unwrap();
        let pb = ProgressBar::new(total_docs as u64);
        pb.set_style(ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
            .unwrap().progress_chars("##-"));

        // Create a semaphore to limit concurrent tasks
        let semaphore = Arc::new(Semaphore::new(25));
        let mut interval = interval(Duration::from_secs(60));

        while let Some(result) = cursor.next().await {
            if let Ok(document) = result {
                if let Some(url) = document.get_str("url").ok() {
                    let url = url.to_string();
                    let id = document.get("_id").unwrap().clone();
                    let collection = collection.clone();
                    let sem = Arc::clone(&semaphore);
                    let pb = pb.clone();

                    tasks.push(tokio::spawn(async move {
                        let _permit = sem.acquire().await;
                        match fetch_text(&url).await {
                            Ok(text) => {
                                if text.split_whitespace().count() < 45 {
                                    let update = doc! { "$set": { "content.text": text } };
                                    if let Err(e) = collection.update_one(doc! { "_id": id }, update).await {
                                        eprintln!("Failed to update document: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("Error fetching text for URL {}: {}", url, e);
                            }
                        }
                        pb.inc(1);
                    }));
                }
            }

            if semaphore.available_permits() == 0 {
                interval.tick().await;
                semaphore.add_permits(rate_limit as usize);
            }
        }

        // Await all the remaining tasks
        while let Some(_) = tasks.next().await {}

        pb.finish_with_message("Text fetching and updating completed");

        // Calculate time until next run
        let elapsed = start_time.elapsed();
        let wait_time = Duration::from_secs(wait_time_per_cycle as u64 * 60).saturating_sub(elapsed);
        
        // Countdown timer
        let mut stdout = stdout();
        for remaining in (0..=wait_time.as_secs()).rev() {
            print!("\rNext run in {:02}:{:02}", remaining / 60, remaining % 60);
            stdout.flush().unwrap();
            std::thread::sleep(Duration::from_secs(1));
        }
        println!("\rStarting next run now!            ");
    }
}