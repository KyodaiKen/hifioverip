use pipewire as pw;
use pw::properties::properties;
use pw::spa;
use spa::pod::Pod;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::mpsc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize the global PipeWire library state
    pw::init();

    // 2. Initialize the MainLoop and Context
    let mainloop = pw::main_loop::MainLoopRc::new(None)?;
    let context = pw::context::ContextRc::new(&mainloop, None)?;
    let core = context.connect_rc(None)?;

    println!("\n==========================================");
    println!("     PIPEWIRE 0.10 RECORDING SERVICE     ");
    println!("==========================================");
    println!("File Format : Sun Audio (.au)");
    println!("Sample Rate : 48000 Hz");
    println!("Bit Depth   : 32-bit Floating Point (f32)");
    println!("Channels    : 2 (Stereo)");
    println!("==========================================\n");

    // 3. Initialize the Sun Audio (.au) file wrapper
    let file = File::create("desktop_capture.au")?;
    let mut file_writer = BufWriter::new(file);

    let magic_bytes: u32 = 0x2e736e64;
    let data_offset: u32 = 24;
    let unknown_size: u32 = 0xffffffff;
    let encoding_s32: u32 = 6;
    let sample_rate: u32 = 48000;
    let channels: u32 = 2;

    file_writer.write_all(&magic_bytes.to_be_bytes())?;
    file_writer.write_all(&data_offset.to_be_bytes())?;
    file_writer.write_all(&unknown_size.to_be_bytes())?;
    file_writer.write_all(&encoding_s32.to_be_bytes())?;
    file_writer.write_all(&sample_rate.to_be_bytes())?;
    file_writer.write_all(&channels.to_be_bytes())?;
    file_writer.flush()?;

    // 4. Set up cross-thread communication
    let (tx, rx) = mpsc::channel::<f32>();

    // 5. Spawn isolated I/O thread
    let writer_handle = std::thread::spawn(move || {
        let mut total_samples = 0u64;
        for sample in rx {
            if file_writer.write_all(&sample.to_be_bytes()).is_ok() {
                total_samples += 1;
            }
        }
        let _ = file_writer.flush();
        total_samples
    });

    // 6. Setup Registry: Find the Sink ID
    let registry = core.get_registry()?;
    let (node_tx, node_rx) = std::sync::mpsc::channel::<(String, u32, bool)>();

    let _registry_listener = registry
    .add_listener_local()
    .global(move |g| {
        if let Some(props) = g.props {
            if let Some(media_class) = props.get("media.class") {
                if media_class == "Audio/Sink" {
                    if let Some(name) = props.get("node.name") {
                        let is_default = props.get("node.default") == Some("true");
                        let _ = node_tx.send((name.to_string(), g.id, is_default));
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

    let (target_sink_name, target_id, _) = sinks.first()
    .expect("!!! No sinks found. Check your DAC connection !!!");

    // Convert the exact ID to a string. This is the safest way to target a node.
    let target_id_str = target_id.to_string();

    println!(">>> TARGET ACQUIRED: Sink ID {} ({})", target_id, target_sink_name);

    // 7. Define properties
    let props = properties! {
        *pw::keys::MEDIA_TYPE => "Audio",
        *pw::keys::MEDIA_CATEGORY => "Capture",

        // >>> FIX 1: Tell WirePlumber we explicitly want to capture from a Sink
        "stream.capture.sink" => "true",

        // >>> FIX 2: Target the absolute Node ID instead of guessing the name
        "target.object" => target_id_str.as_str(),

        "node.name" => "desktop-audio-capture",
        "media.class" => "Stream/Input/Audio",
        *pw::keys::APP_NAME => "Desktop Audio Capture Tool",
        "audio.channels" => "2",
        "audio.position" => "FL,FR",
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
                let samples_count = active_audio.len() / 4;

                for i in 0..samples_count {
                    let chunk = &active_audio[i*4..(i*4)+4];
                    let sample = f32::from_le_bytes(chunk.try_into().unwrap());
                    let _ = stream_tx.send(sample);
                }
            }
        }
    })
    .register()?;

    // 11. Define format constraints
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
            spa::param::format::FormatProperties::AudioFormat, Id, spa::param::audio::AudioFormat::F32LE
        ),
        spa::pod::property!(
            spa::param::format::FormatProperties::AudioRate, Int, sample_rate as i32
        ),
        spa::pod::property!(
            spa::param::format::FormatProperties::AudioChannels, Int, channels as i32
        )
    );

    let values: Vec<u8> = spa::pod::serialize::PodSerializer::serialize(
        std::io::Cursor::new(Vec::new()),
                                                                        &spa::pod::Value::Object(obj),
    )
    .unwrap().0.into_inner();

    let mut params = [Pod::from_bytes(&values).unwrap()];

    // 12. Connect using Direction::Input and AUTOCONNECT
    stream.connect(
        spa::utils::Direction::Input,
        None, // 'None' forces it to use "target.object" from our properties! macro
        pw::stream::StreamFlags::AUTOCONNECT | pw::stream::StreamFlags::MAP_BUFFERS,
        &mut params,
    )?;

    // 13. Set active
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

    // 15. Orderly destruction
    stream.disconnect()?;
    drop(stream);
    drop(_listener);
    drop(_channel_receiver);
    drop(tx);

    let final_sample_count = writer_handle.join().unwrap_or(0);

    println!("\n==========================================");
    println!("             RECORDING REPORT             ");
    println!("==========================================");
    println!("File State   : Closed and Written successfully.");
    println!("Total Samples: {} frames saved", final_sample_count);
    println!("Output File  : desktop_capture.au");
    println!("==========================================\n");

    Ok(())
}
