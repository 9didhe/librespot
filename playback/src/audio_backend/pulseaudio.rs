use super::{Open, Sink, SinkAsBytes, SinkError, SinkResult};
use crate::config::AudioFormat;
use crate::convert::Converter;
use crate::decoder::AudioPacket;
use crate::listening::RenderedAudio;
use crate::rendered_queue::RenderedQueue;
use crate::{NUM_CHANNELS, SAMPLE_RATE};
use libpulse_binding::{self as pulse, error::PAErr, stream::Direction};
use libpulse_simple_binding::Simple;
use std::{
    env,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};
use thiserror::Error;

#[derive(Debug, Error)]
enum PulseError {
    #[error(
        "<PulseAudioSink> Unsupported Pulseaudio Sample Spec, Format {pulse_format:?} ({format:?}), Channels {channels}, Rate {rate}"
    )]
    InvalidSampleSpec {
        pulse_format: pulse::sample::Format,
        format: AudioFormat,
        channels: u8,
        rate: u32,
    },

    #[error("<PulseAudioSink> {0}")]
    ConnectionRefused(PAErr),

    #[error("<PulseAudioSink> Failed to Drain Pulseaudio Buffer, {0}")]
    DrainFailure(PAErr),

    #[error("<PulseAudioSink>")]
    NotConnected,

    #[error("<PulseAudioSink> {0}")]
    OnWrite(PAErr),
}

impl From<PulseError> for SinkError {
    fn from(e: PulseError) -> SinkError {
        use PulseError::*;
        let es = e.to_string();
        match e {
            DrainFailure(_) | OnWrite(_) => SinkError::OnWrite(es),
            ConnectionRefused(_) => SinkError::ConnectionRefused(es),
            NotConnected => SinkError::NotConnected(es),
            InvalidSampleSpec { .. } => SinkError::InvalidParams(es),
        }
    }
}

pub struct PulseAudioSink {
    sink: Option<Arc<PulseOutput>>,
    rendered_audio: Option<Arc<RenderedAudio>>,
    device: Option<String>,
    app_name: String,
    stream_desc: String,
    format: AudioFormat,
}

struct PulseOutput {
    stream: Simple,
    clock: Mutex<RenderedQueue>,
    closed: AtomicBool,
    timing_failed: AtomicBool,
}

impl PulseOutput {
    fn observe(&self) -> Result<(), PAErr> {
        let submitted = self.clock.lock().unwrap().submitted();
        // Simple is Send + Sync and protects its Pulse mainloop internally.
        // Do not hold our FIFO mutex across this potentially blocking call.
        let latency = self.stream.get_latency()?;
        self.clock.lock().unwrap().observe(submitted, latency.0);
        Ok(())
    }

    fn discard(&self) {
        self.closed.store(true, Ordering::Release);
        self.clock.lock().unwrap().discard();
    }
}

fn observe_playback(output: Weak<PulseOutput>) {
    while let Some(output) = output.upgrade() {
        if output.closed.load(Ordering::Acquire) {
            break;
        }
        let pending = output.clock.lock().unwrap().pending();
        let timing = if pending { output.observe() } else { Ok(()) };
        if let Err(error) = timing {
            warn!("Unable to read PulseAudio playback timing: {error}");
            // Timing is only for reporting. Losing it must not interrupt a
            // stream that can still play; discard unconfirmed frames instead.
            output.timing_failed.store(true, Ordering::Release);
            output.clock.lock().unwrap().discard();
            break;
        }
        // No strong output reference is kept between ticks. A stalled native
        // call may outlive shutdown, so this observer is never joined by Drop.
        drop(output);
        thread::sleep(Duration::from_millis(10));
    }
}

impl Open for PulseAudioSink {
    fn open(device: Option<String>, format: AudioFormat) -> Self {
        let app_name = env::var("PULSE_PROP_application.name").unwrap_or_default();
        let stream_desc = env::var("PULSE_PROP_stream.description").unwrap_or_default();

        let mut actual_format = format;

        if actual_format == AudioFormat::F64 {
            warn!("PulseAudio currently does not support F64 output");
            actual_format = AudioFormat::F32;
        }

        info!("Using PulseAudioSink with format: {actual_format:?}");

        Self {
            sink: None,
            rendered_audio: None,
            device,
            app_name,
            stream_desc,
            format: actual_format,
        }
    }
}

impl Sink for PulseAudioSink {
    fn set_rendered_audio(&mut self, audio: Option<Arc<RenderedAudio>>) -> bool {
        self.rendered_audio = audio;
        true
    }

    fn start(&mut self) -> SinkResult<()> {
        if self.sink.is_none() {
            // PulseAudio calls S24 and S24_3 different from the rest of the world
            let pulse_format = match self.format {
                AudioFormat::F32 => pulse::sample::Format::FLOAT32NE,
                AudioFormat::S32 => pulse::sample::Format::S32NE,
                AudioFormat::S24 => pulse::sample::Format::S24_32NE,
                AudioFormat::S24_3 => pulse::sample::Format::S24NE,
                AudioFormat::S16 => pulse::sample::Format::S16NE,
                _ => unreachable!(),
            };

            let sample_spec = pulse::sample::Spec {
                format: pulse_format,
                channels: NUM_CHANNELS,
                rate: SAMPLE_RATE,
            };

            if !sample_spec.is_valid() {
                let pulse_error = PulseError::InvalidSampleSpec {
                    pulse_format,
                    format: self.format,
                    channels: NUM_CHANNELS,
                    rate: SAMPLE_RATE,
                };

                return Err(SinkError::from(pulse_error));
            }

            let sink = Simple::new(
                None,                   // Use the default server.
                &self.app_name,         // Our application's name.
                Direction::Playback,    // Direction.
                self.device.as_deref(), // Our device (sink) name.
                &self.stream_desc,      // Description of our stream.
                &sample_spec,           // Our sample format.
                None,                   // Use default channel map.
                None,                   // Use default buffering attributes.
            )
            .map_err(PulseError::ConnectionRefused)?;

            let output = Arc::new(PulseOutput {
                stream: sink,
                clock: Mutex::new(RenderedQueue::new(SAMPLE_RATE)),
                closed: AtomicBool::new(false),
                timing_failed: AtomicBool::new(false),
            });
            let observer = Arc::downgrade(&output);
            thread::Builder::new()
                .name("librespot-pulse-clock".into())
                .spawn(move || observe_playback(observer))
                .map_err(|error| SinkError::ConnectionRefused(error.to_string()))?;
            self.sink = Some(output);
        }

        Ok(())
    }

    fn stop(&mut self) -> SinkResult<()> {
        let sink = self.sink.take().ok_or(PulseError::NotConnected)?;

        match sink.stream.drain() {
            Ok(()) => {
                sink.closed.store(true, Ordering::Release);
                sink.clock.lock().unwrap().drained();
                Ok(())
            }
            Err(error) => {
                sink.discard();
                Err(PulseError::DrainFailure(error).into())
            }
        }
    }

    sink_as_bytes!();
}

impl SinkAsBytes for PulseAudioSink {
    #[inline]
    fn write_bytes(&mut self, data: &[u8]) -> SinkResult<()> {
        let sink = self.sink.as_mut().ok_or(PulseError::NotConnected)?;

        if sink.closed.load(Ordering::Acquire) {
            return Err(PulseError::NotConnected.into());
        }
        if let Err(error) = sink.stream.write(data) {
            sink.discard();
            return Err(PulseError::OnWrite(error).into());
        }
        let bytes_per_sample = match self.format {
            AudioFormat::F32 | AudioFormat::S32 | AudioFormat::S24 => 4,
            AudioFormat::S24_3 => 3,
            AudioFormat::S16 => 2,
            _ => unreachable!(),
        };
        let frames = data.len() / (bytes_per_sample * usize::from(NUM_CHANNELS));
        let mut clock = sink.clock.lock().unwrap();
        if sink.closed.load(Ordering::Acquire) {
            return Err(PulseError::NotConnected.into());
        }
        if !sink.timing_failed.load(Ordering::Acquire) {
            clock.submit(frames as u64, self.rendered_audio.as_ref());
        }

        Ok(())
    }
}

impl Drop for PulseAudioSink {
    fn drop(&mut self) {
        if let Some(sink) = &self.sink {
            sink.discard();
        }
    }
}

impl PulseAudioSink {
    pub const NAME: &'static str = "pulseaudio";
}
