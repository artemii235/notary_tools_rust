use chain::constants::SEQUENCE_FINAL;
use chain::{OutPoint, TransactionOutput};
use coins::utxo::rpc_clients::{electrum_script_hash, ElectrumUnspent, UtxoRpcClientEnum, UtxoRpcClientOps};
use coins::utxo::utxo_standard::{utxo_standard_coin_from_conf_and_request, UtxoStandardCoin};
use coins::utxo::{p2pk_spend, Address, UtxoTx};
use coins::MarketCoinOps;
use common::block_on;
use common::mm_ctx::MmCtxBuilder;
use common::mm_error::prelude::*;
use common::privkey::key_pair_from_seed;
use common::serde_derive::Deserialize;
use common::serde_json::{self as json, Value as Json};
use futures01::Future;
use script::{Builder, UnsignedTransactionInput};
use serialization::serialize;
use std::time::Duration;

fn unsigned_input_from_electrum(el: &ElectrumUnspent) -> UnsignedTransactionInput {
    UnsignedTransactionInput {
        previous_output: OutPoint {
            hash: el.tx_hash.reversed().into(),
            index: el.tx_pos,
        },
        sequence: SEQUENCE_FINAL,
        amount: el.value,
    }
}

#[derive(Debug, Deserialize)]
struct CoinConf {
    ticker: String,
    activation_command: Json,
    output_threshold: u64,
    mm_conf: Json,
}

#[derive(Debug, Deserialize)]
struct MergerConfig {
    seeds: Vec<String>,
    send_to_address: String,
    coins: Vec<CoinConf>,
}

#[derive(Debug)]
enum MainError {
    ConfFileRead(std::io::Error),
    ConfSerde(json::Error),
    KeysError(keys::Error),
    String(String),
}

impl From<std::io::Error> for MainError {
    fn from(err: std::io::Error) -> MainError { MainError::ConfFileRead(err) }
}

impl From<json::Error> for MainError {
    fn from(err: json::Error) -> MainError { MainError::ConfSerde(err) }
}

impl From<keys::Error> for MainError {
    fn from(err: keys::Error) -> MainError { MainError::KeysError(err) }
}

impl From<String> for MainError {
    fn from(err: String) -> MainError { MainError::String(err) }
}

fn main() -> Result<(), MmError<MainError>> {
    let conf_path = "./merger.json";
    let content = std::fs::read_to_string(conf_path)?;
    let conf: MergerConfig = json::from_str(&content)?;

    let to_address: Address = conf.send_to_address.parse()?;
    let keypairs: Result<Vec<_>, _> = conf.seeds.iter().map(|seed| key_pair_from_seed(&seed)).collect();
    let keypairs = keypairs?;

    let ctx = MmCtxBuilder::default().into_mm_arc();

    // init with dummy privkey as signing is done separately
    let coins: Result<Vec<(UtxoStandardCoin, u64)>, String> = conf
        .coins
        .iter()
        .map(|coin| {
            Ok((
                block_on(utxo_standard_coin_from_conf_and_request(
                    &ctx,
                    &coin.ticker,
                    &coin.mm_conf,
                    &coin.activation_command,
                    &[1; 32],
                ))?,
                coin.output_threshold,
            ))
        })
        .collect();
    let coins = coins?;

    loop {
        for (coin, output_threshold) in coins.iter() {
            let electrum = match &coin.as_ref().rpc_client {
                UtxoRpcClientEnum::Electrum(electrum) => electrum,
                _ => panic!("Merger works only with Electrum client"),
            };
            let current_block = match electrum.get_block_count().wait() {
                Ok(b) => b,
                Err(e) => {
                    println!("Error {} on getting block number for the coin {}", e, coin.ticker());
                    continue;
                },
            };
            let mut unspents_with_priv = vec![];
            for keypair in keypairs.iter() {
                let script = Builder::build_p2pk(keypair.public());
                let hash = electrum_script_hash(&script);
                let hash_str = hex::encode(hash);

                let unspents = match electrum.scripthash_list_unspent(&hash_str).wait() {
                    Ok(u) => u,
                    Err(e) => {
                        println!("Error {} on getting unspents for public key {}", e, keypair.public());
                        continue;
                    },
                };
                unspents_with_priv.extend(unspents.into_iter().map(|u| (u, keypair)));
            }

            unspents_with_priv.retain(|(unspent, _)| {
                let value_match = unspent.value >= *output_threshold;
                let is_mature = match unspent.height {
                    Some(tx_height) => current_block - tx_height > 100,
                    None => false,
                };
                value_match && is_mature
            });

            if unspents_with_priv.len() < 4 {
                println!("Currently available unspents {}, skipping", unspents_with_priv.len());
                continue;
            }

            let mut unsigned = coin.as_ref().transaction_preimage();
            unsigned.inputs = unspents_with_priv
                .iter()
                .map(|(el, _)| unsigned_input_from_electrum(el))
                .collect();

            let script_pubkey = Builder::build_p2pkh(&to_address.hash).to_bytes();

            let output_amount = unsigned.inputs.iter().fold(0, |cur, input| cur + input.amount - 1000);
            let output = TransactionOutput {
                value: output_amount,
                script_pubkey,
            };

            unsigned.outputs = vec![output];

            let signed_inputs: Result<Vec<_>, _> = unsigned
                .inputs
                .iter()
                .enumerate()
                .map(|(i, _)| {
                    p2pk_spend(
                        &unsigned,
                        i,
                        &unspents_with_priv[i].1,
                        coin.as_ref().conf.signature_version,
                        coin.as_ref().conf.fork_id,
                    )
                })
                .collect();

            let signed_inputs = match signed_inputs {
                Ok(s) => s,
                Err(e) => {
                    println!(
                        "Error {} on signing the tx {:?} for coin {}",
                        e,
                        unsigned,
                        coin.ticker()
                    );
                    continue;
                },
            };

            let mut signed_tx: UtxoTx = unsigned.into();
            signed_tx.inputs = signed_inputs;

            let bytes = serialize(&signed_tx);
            let hex = hex::encode(&bytes);
            let hash = match coin.send_raw_tx(&hex).wait() {
                Ok(h) => h,
                Err(e) => {
                    println!("Error {} on sending {} transaction {}", e, coin.ticker(), hex);
                    continue;
                },
            };
            println!("Sent {} transaction {}", coin.ticker(), hash);
        }

        println!("Sleeping for 15 minutes");
        std::thread::sleep(Duration::from_secs(15 * 60));
    }
}
