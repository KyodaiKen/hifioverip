use pipewire as pw;
use pw::properties::properties;
use pw::spa;
use spa::pod::Pod;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::mpsc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ==========================================
    // COMMAND LINE PARSER
    // ==========================================
    let mut config_sample_rate: u32 = 48000;
    let mut config_bit_depth: u32 = 24;
    let mut config_auto_channels: bool = true;
    let mut config_fallback_channels: u32 = 2;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-s" => {
                if let Some(val) = args.next() {
                    config_sample_rate = val.parse().expect("Invalid sample rate provided to -s");
                }
            }
            "-b" => {
                if let Some(val) = args.next() {
                    config_bit_depth = val.parse().expect("Invalid bit depth provided to -b");
                }
            }
            "-c" => {
                if let Some(val) = args.next() {
                    if val.to_lowercase() == "auto" {
                        config_auto_channels = true;
                    } else {
                        config_auto_channels = false;
                        config_fallback_channels = val.parse().expect("Invalid channel count provided to -c");
                    }
                }
            }
            "-h" | "--help" => {
                println!("Usage: capture [OPTIONS]");
                println!("  -s <rate>    Sample rate (default: 48000)");
                println!("  -b <bits>    Bit depth: 16, 24, 32 (default: 24)");
                println!("  -c <count>   Channels: 'auto' or number (default: auto)");
                return Ok(());
            }
            _ => {
                println!("Unknown argument: {}. Use -h for help.", arg);
                return Ok(());
            }
        }
    }

    assert!(
        [16, 24, 32].contains(&config_bit_depth),
            "Only 16, 24, and 32-bit integer depths are supported."
    );

    // 1. Initialize the global PipeWire library state
    pw::init();

    // 2. Initialize the MainLoop and Context
    let mainloop = pw::main_loop::MainLoopRc::new(None)?;
    let context = pw::context::ContextRc::new(&mainloop, None)?;
    let core = context.connect_rc(None)?;

    // 3. Setup Registry: Find the Sink ID and its channel count
    let registry = core.get_registry()?;
    let (node_tx, node_rx) = std::sync::mpsc::channel::<(String, u32, bool, Option<u32>)>();

    let _registry_listener = registry
    .add_listener_local()
    .global(move |g| {
        if let Some(props) = g.props {
            if let Some(media_class) = props.get("media.class") {
                if media_class == "Audio/Sink" {
                    if let Some(name) = props.get("node.name") {
                        let is_default = props.get("node.default") == Some("true");

                        let mut detected_ch = None;

                        // Check 1: Explicit audio.channels property
                        if let Some(ch_str) = props.get("audio.channels") {
                            if let Ok(c) = ch_str.parse::<u32>() {
                                detected_ch = Some(c);
                            }
                        }

                        // Check 2: Fallback to counting speakers in audio.position (e.g. "FL,FR" -> 2)
                        if detected_ch.is_none() {
                            if let Some(pos_str) = props.get("audio.position") {
                                let count = pos_str.split(',').count() as u32;
                                if count > 0 {
                                    detected_ch = Some(count);
                                }
                            }
                        }

                        let _ = node_tx.send((name.to_string(), g.id, is_default, detected_ch));
                    }
                }
            }
        }
    })
    .register();

    // Give it enough time to hear the registry
    for _ in 0..10 {
        mainloop.loop_().iterate(pipewire::loop_::Timeout::Finite(std::time::Duration::from_millis(100)));
    }

    // Process the list
    let mut sinks = Vec::new();
    while let Ok(data) = node_rx.try_recv() {
        sinks.push(data);
    }

    // Sort logic: Pro Audio gets priority 2, Analog gets 1
    sinks.sort_by(|a, b| {
        let score = |name: &str| if name.contains("pro-output") { 2 } else { 1 };
        score(&b.0).cmp(&score(&a.0))
    });

    let (target_sink_name, target_id, _, detected_channels) = sinks.first()
    .expect("!!! No sinks found. Check your DAC connection !!!");

    let target_id_str = target_id.to_string();

    // Determine final channel count
    let final_channels = if config_auto_channels {
        detected_channels.unwrap_or(config_fallback_channels)
    } else {
        config_fallback_channels
    };

    println!("\n==========================================");
    println!("     PIPEWIRE 0.10 RECORDING SERVICE     ");
    println!("==========================================");
    println!("File Format : Sun Audio (.au)");
    println!("Sample Rate : {} Hz", config_sample_rate);
    println!("Bit Depth   : {}-bit Integer PCM", config_bit_depth);
    println!("Channels    : {} ({})", final_channels,
             if !config_auto_channels { "Manual Override" }
             else if detected_channels.is_some() { "Auto-detected" }
             else { "Auto-fallback" }
    );
    println!("Target      : ID {} ({})", target_id, target_sink_name);
    println!("==========================================\n");

    // 4. Initialize the Sun Audio (.au) file wrapper NOW that we know the channels
    let file = File::create("desktop_capture.au")?;
    let mut file_writer = BufWriter::new(file);

    let magic_bytes: u32 = 0x2e736e64;
    let data_offset: u32 = 24;
    let unknown_size: u32 = 0xffffffff;

    // Map our bit depth to the Sun Audio encoding format
    let encoding: u32 = match config_bit_depth {
        16 => 3, // 16-bit linear PCM
        24 => 4, // 24-bit linear PCM
        32 => 5, // 32-bit linear PCM
        _ => unreachable!(),
    };

    file_writer.write_all(&magic_bytes.to_be_bytes())?;
    file_writer.write_all(&data_offset.to_be_bytes())?;
    file_writer.write_all(&unknown_size.to_be_bytes())?;
    file_writer.write_all(&encoding.to_be_bytes())?;
    file_writer.write_all(&config_sample_rate.to_be_bytes())?;
    file_writer.write_all(&final_channels.to_be_bytes())?;
    file_writer.flush()?;

    // 5. Set up cross-thread communication using Vec<u8> byte-chunks
    let (tx, rx) = mpsc::channel::<Vec<u8>>();

    // 6. Spawn isolated I/O thread
    let writer_handle = std::thread::spawn(move || {
        let mut total_bytes = 0u64;
        for chunk in rx {
            if file_writer.write_all(&chunk).is_ok() {
                total_bytes += chunk.len() as u64;
            }
        }
        let _ = file_writer.flush();
        total_bytes
    });

    // 7. Define properties
    //let channels_str = final_channels.to_string();
    let props = properties! {
        *pw::keys::MEDIA_TYPE => "Audio",
        *pw::keys::MEDIA_CATEGORY => "Capture",
        "stream.capture.sink" => "true",
        "target.object" => target_id_str.as_str(),
        "node.name" => "desktop-audio-capture",
        "media.class" => "Stream/Input/Audio",
        *pw::keys::APP_NAME => "Desktop Audio Capture Tool",
        //"audio.channels" => channels_str.as_str(),
    };

    // 8. Instantiate the stream
    let stream = pw::stream::StreamBox::new(&core, "desktop-audio-capture", props)?;

    // 9. Construct the listener
    let stream_tx = tx.clone();

    let _listener = stream
    .add_local_listener_with_user_data(())
    .state_changed(move |_, _, old, new| {
        match new {
            pw::stream::StreamState::Error(err) => println!("!!! STREAM ERROR: {} !!!", err),
                   _ => println!(">>> Stream status transition: {:?} -> {:?}", old, new),
        }
    })
    .process(move |stream, _| {
        if let Some(mut buffer) = stream.dequeue_buffer() {
            let datas = buffer.datas_mut();
            if datas.is_empty() { return; }

            let data = &mut datas[0];
            let valid_size = data.chunk().size() as usize;

            if let Some(bytes) = data.data() {
                let active_audio = &bytes[..valid_size.min(bytes.len())];
                let mut out_bytes = Vec::with_capacity(active_audio.len());

                match config_bit_depth {
                    16 => {
                        let samples_count = active_audio.len() / 2;
                        for i in 0..samples_count {
                            let chunk = &active_audio[i*2..(i*2)+2];
                            let sample = i16::from_le_bytes(chunk.try_into().unwrap());
                            out_bytes.extend_from_slice(&sample.to_be_bytes()); // Push BE to file
                        }
                    },
                    24 => {
                        // We requested S32LE, so process 4 bytes at a time
                        let samples_count = active_audio.len() / 4;
                        for i in 0..samples_count {
                            let chunk = &active_audio[i*4..(i*4)+4];
                            let sample = i32::from_le_bytes(chunk.try_into().unwrap());

                            // Shift down to extract highest 24 bits
                            let sample_24 = sample >> 8;
                            let be_bytes = sample_24.to_be_bytes();

                            // Write 3 big-endian bytes to the file
                            out_bytes.extend_from_slice(&be_bytes[1..4]);
                        }
                    },
                    32 => {
                        let samples_count = active_audio.len() / 4;
                        for i in 0..samples_count {
                            let chunk = &active_audio[i*4..(i*4)+4];
                            let sample = i32::from_le_bytes(chunk.try_into().unwrap());
                            out_bytes.extend_from_slice(&sample.to_be_bytes()); // Push BE to file
                        }
                    },
                    _ => unreachable!(),
                }

                if !out_bytes.is_empty() {
                    let _ = stream_tx.send(out_bytes);
                }
            }
        }
    })
    .register()?;

    // 10. Define format constraints dynamically
    let pw_format = match config_bit_depth {
        16 => spa::param::audio::AudioFormat::S16LE,
        24 => spa::param::audio::AudioFormat::S32LE,
        32 => spa::param::audio::AudioFormat::S32LE,
        _ => unreachable!(),
    };

    let obj = spa::pod::object!(
        spa::utils::SpaTypes::ObjectParamFormat,
        spa::param::ParamType::EnumFormat,
        spa::pod::property!(
            spa::param::format::FormatProperties::MediaType, Id, spa::param::format::MediaType::Audio
        ),
        spa::pod::property!(
            spa::param::format::FormatProperties::MediaSubtype, Id, spa::param::format::MediaSubtype::Raw
        ),
        spa::pod::property!(
            spa::param::format::FormatProperties::AudioFormat, Id, pw_format
        ),
        spa::pod::property!(
            spa::param::format::FormatProperties::AudioRate, Int, config_sample_rate as i32
        ),
        spa::pod::property!(
            spa::param::format::FormatProperties::AudioChannels, Int, final_channels as i32
        )
    );

    let values: Vec<u8> = spa::pod::serialize::PodSerializer::serialize(
        std::io::Cursor::new(Vec::new()),
                                                                        &spa::pod::Value::Object(obj),
    )
    .unwrap().0.into_inner();

    let mut params = [Pod::from_bytes(&values).unwrap()];

    // 11. Connect using Direction::Input and AUTOCONNECT
    stream.connect(
        spa::utils::Direction::Input,
        None,
        pw::stream::StreamFlags::AUTOCONNECT | pw::stream::StreamFlags::MAP_BUFFERS,
        &mut params,
    )?;

    // 12. Set active
    stream.set_active(true)?;

    let (pw_sender, pw_receiver) = pipewire::channel::channel::<()>();
    let loop_clone = mainloop.clone();
    let _channel_receiver = pw_receiver.attach(mainloop.loop_(), move |_| {
        loop_clone.quit();
    });

    println!("\n>>> Recording is active. Play some audio on your desktop! <<<");
    println!("Press [ENTER] to stop recording and finalize the file safely...");

    std::thread::spawn(move || {
        let mut exit_buffer = String::new();
        let _ = std::io::stdin().read_line(&mut exit_buffer);
        println!("Shutting down audio capture stream...");
        let _ = pw_sender.send(());
    });

    mainloop.run();

    // 13. Orderly destruction
    stream.disconnect()?;
    drop(stream);
    drop(_listener);
    drop(_channel_receiver);
    drop(tx);

    let final_bytes_written = writer_handle.join().unwrap_or(0);

    let bytes_per_sample_frame = (config_bit_depth / 8) * final_channels;
    let final_sample_count = final_bytes_written / bytes_per_sample_frame as u64;

    println!("\n==========================================");
    println!("             RECORDING REPORT             ");
    println!("==========================================");
    println!("File State   : Closed and Written successfully.");
    println!("Total Frames : {} audio frames saved", final_sample_count);
    println!("Output File  : desktop_capture.au");
    println!("==========================================\n");

    Ok(())
}
