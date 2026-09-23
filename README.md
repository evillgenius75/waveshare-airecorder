# Castor

Castor is a voice-first thought-capture companion on an always-on e-paper screen: record ideas, to-dos, and notes the moment they arrive, let Gemini transcribe and summarize them, and keep the ones that matter in front of you as sticky notes. It's a quiet, always-visible place for your thoughts instead of one more app buried in your phone. Castor is derived from the Followup project.

## What it's for

- Catching a sudden idea by voice before it slips away
- Jotting quick to-dos and notes hands-free while you're busy with something else
- Keeping a small, always-visible set of follow-ups on a desk, fridge, or wall
- Revisiting past ideas later to decide what's still worth pursuing

## Key features

- **Capture at the light-bulb moment.** Hold BOOT and speak, then tag the recording as an Idea, a To-do, or a Note. Recordings are currently up to 10 seconds; longer ones are coming.
- **Gemini transcription and summaries.** Gemini turns each recording into readable text, and can summarize your recent notes and to-dos into a short, scannable overview.
- **Your notes stay yours.** Recordings, transcripts, and summaries are saved on the microSD card in the device.
- **Vibe check your ideas.** Review each idea and decide whether it's still worth keeping or ready for the trash.
- **Follow-ups.** Flag the to-dos and notes that matter so they stay on your radar until they're done.
- **Stickies.** Pin your follow-ups to the screen as sticky notes, a steady, low-interruption reminder of what's next.

## Controls

Castor has no touchscreen; everything runs on a rocker and two buttons.

| Control | Action |
| --- | --- |
| Rocker up / down | Move the selection; hold to repeat |
| Rocker down, held | Back out of a list or card you've entered |
| Rocker middle, or BOOT tap | Select / confirm |
| BOOT, press and hold | Record: starts when you hold, stops when you let go |
| PWR, tap | Lock or unlock the screen |
| PWR, hold about 1 second | Shut down (asks to confirm) |
| PWR, hold 6 seconds | Force power-off |

Only BOOT records, so no other button can start a recording by accident.

## Specifications

| Item | Details |
| --- | --- |
| Hardware | [Waveshare ESP32-S3-ePaper-3.97](https://docs.waveshare.com/ESP32-S3-ePaper-3.97) |
| Screen | 3.97-inch black-and-white e-paper, 800 x 480 |
| Controls | Rocker plus BOOT and PWR buttons |
| Connectivity | 2.4 GHz Wi-Fi, Bluetooth LE |
| Audio | Built-in microphone, speaker connector |
| Storage | microSD card |
| Power | Rechargeable lithium battery, USB-C charging |
| AI | Gemini over Wi-Fi; needs a Gemini API key |

## Setting up Castor

You'll need a free Gemini API key from [Google AI Studio](https://aistudio.google.com/). The free tier works within Gemini's limits; a paid account removes them.

**Today:** when Castor has no Wi-Fi saved, it creates its own setup Wi-Fi network. Join it from your phone or computer, and a setup page opens where you enter your Wi-Fi network, Gemini API key, and timezone.

**Coming soon, the Castor setup app:** scan the QR code on Castor's screen with an Android phone or Chrome on a computer, confirm the 6-digit code Castor shows, pick your Wi-Fi, and paste your API key. Your timezone is set from your phone automatically. iPhone support follows.

The Castor setup app lives on GitHub Pages for now, moving to a dedicated product site at launch.

## What's coming

Updates arrive in this order, starting with reliability and safety so everything after is built on solid ground.

| Stage | What you get |
| --- | --- |
| 1. Solid foundations | Recordings stay safe even if power is lost mid-save; setup requires the code on Castor's screen; your API key is stored encrypted |
| 2. Capture anything, anytime | Longer recordings; record again right away while the last one is still transcribing; record offline and get transcripts automatically once back online |
| 3. Setup from your phone | The Castor setup app for Android phones and Chrome on computers |
| 4. Smarter results | Faster transcripts; automatic titles and tags; to-dos and due dates picked up from what you say |
| 5. Always-on stickies | Stickies stay on screen even while Castor sleeps; longer battery life |
| 6. iPhone app and updates | Castor apps for iPhone and Android; software updates over Wi-Fi, no cable needed |
| 7. Reminders | Castor chimes when a to-do is due; a weekly review; ask questions about your notes |
| 8. Your notes everywhere | Browse and edit notes in the app; export to Markdown, Google Tasks, Google Calendar, and Notion |
