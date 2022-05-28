use flate2::read::GzDecoder;
use lambda_debug_proxy_client::{init_tracing, RequestPayload};
use lambda_runtime::{service_fn, Error, LambdaEvent};
use rusoto_core::region::Region;
use rusoto_sqs::{DeleteMessageRequest, ReceiveMessageRequest, SendMessageRequest, Sqs, SqsClient};
use serde_json::Value;
use std::env::var;
use std::io::Read;
use std::str::FromStr;
use tracing::debug;

#[tokio::main]
async fn main() -> Result<(), Error> {
    init_tracing(None);

    lambda_runtime::run(service_fn(my_handler)).await?;

    Ok(())
}

async fn my_handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
    let (event, ctx) = event.into_parts();
    debug!("Event: {:?}", event);
    debug!("Context: {:?}", ctx);

    let request_queue_url = var("LAMBDA_PROXY_REQ_QUEUE_URL").expect("Missing LAMBDA_PROXY_REQ_QUEUE_URL");

    debug!("ReqQ URL: {}", request_queue_url);

    let region = Region::default();
    debug!("Region: {:?}", region);
    let client = SqsClient::new(region);

    // Sending part
    let request_payload = RequestPayload { event, ctx };

    let message_body = serde_json::to_string(&request_payload).expect("Failed to serialize event + context");
    debug!("Message body: {}", message_body);

    let send_result = client
        .send_message(SendMessageRequest {
            message_body,
            queue_url: request_queue_url,
            ..Default::default()
        })
        .await?;

    let msg_id = send_result.message_id.unwrap_or_default();
    debug!("Sent with ID: {}", msg_id);

    // start listening to the response if the response queue was specified
    // otherwise exit with OK status for an async request
    if let Ok(response_queue_url) = var("LAMBDA_PROXY_RESP_QUEUE_URL") {
        debug!("RespQ URL {}", response_queue_url);
        // clear the response queue to avoid getting a stale message from a previously timed out request
        // this call limits the invocations to no more than 1 per minute because AWS does not allow purging queues more often
        purge_response_queue(&client, &response_queue_url).await?;
        // now start listening
        loop {
            debug!("20s loop");
            let resp = client
                .receive_message(ReceiveMessageRequest {
                    max_number_of_messages: Some(1),
                    queue_url: response_queue_url.clone(),
                    wait_time_seconds: Some(20),
                    ..Default::default()
                })
                .await?;

            // wait until a message arrives or the function is killed by AWS
            if resp.messages.is_none() {
                debug!("No messages yet");
                continue;
            }

            // an empty list returns when the queue wait time expires
            let mut msgs = resp.messages.expect("Failed to get list of messages");
            if msgs.len() == 0 {
                debug!("No messages yet");
                continue;
            }

            // message arrived - grab its handle for future reference
            let receipt_handle = msgs[0]
                .receipt_handle
                .as_ref()
                .expect("Failed to get msg receipt")
                .to_owned();

            let body = msgs
                .pop()
                .expect("msgs Vec should have been pre-checked for len(). It's a bug.")
                .body
                .expect("Failed to get message body");
            debug!("Response:{}", body);

            let body = decode_maybe_binary(body);

            // delete it from the queue so it's not picked up again
            client
                .delete_message(DeleteMessageRequest {
                    queue_url: response_queue_url.clone(),
                    receipt_handle,
                })
                .await?;
            debug!("Message deleted");

            // return the contents of the message as JSON Value
            return Ok(Value::from_str(&body)?);
        }
    } else {
        debug!("Async invocation. Not waiting for a response from the remote handler.");
        return Ok(Value::Null);
    }
}

/// Checks if the message is a Base58 encoded compressed text and either decodes/decompresses it
/// or returns as-is if it's not encoded/compressed.
fn decode_maybe_binary(body: String) -> String {
    // check for presence of { at the beginning of the doc to determine if it's JSON or Base58
    if body.len() == 0 || body.trim_start().starts_with("{") || body.trim() == "null" {
        // looks like JSON - return as-is
        return body;
    }

    // try to decode base58
    let body_decoded = bs58::decode(&body)
        .into_vec()
        .expect("Failed to decode from maybe base58");

    // try to decompress the body
    let mut decoder = GzDecoder::new(body_decoded.as_slice());
    let mut decoded: Vec<u8> = Vec::new();
    let len = decoder
        .read_to_end(&mut decoded)
        .expect("Failed to decompress the payload");

    debug!("Decoded {} bytes", len);

    // return the bytes converted into a lossy unicode string
    String::from_utf8(decoded).expect("Failed to convert decompressed payload to UTF8")
}

async fn purge_response_queue(client: &SqsClient, response_queue_url: &String) -> Result<(), Error> {
    debug!("Purging the queue, one msg at a time.");
    loop {
        let resp = client
            .receive_message(ReceiveMessageRequest {
                max_number_of_messages: Some(10),
                queue_url: response_queue_url.clone(),
                wait_time_seconds: Some(0),
                ..Default::default()
            })
            .await?;

        // wait until a message arrives or the function is killed by AWS
        if resp.messages.is_none() {
            debug!("No stale messages (resp.messages.is_none)");
            return Ok(());
        }

        // an empty list returns when the queue wait time expires
        let msgs = resp.messages.expect("Failed to get list of messages");
        if msgs.is_empty() {
            debug!("No stale messages (resp.messages.is_empty)");
            return Ok(());
        }

        debug!("Deleting {} stale messages", msgs.len());

        for msg in msgs {
            // delete it from the queue
            client
                .delete_message(DeleteMessageRequest {
                    queue_url: response_queue_url.clone(),
                    receipt_handle: msg.receipt_handle.as_ref().expect("Failed to get msg receipt").into(),
                })
                .await?;
            debug!("Message deleted");
        }
    }
}
