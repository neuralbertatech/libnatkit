#pragma once

#include <atomic>

namespace nat {
namespace tools {

// Whether a recording is in progress, visible across translation units.
//
// --- Why this exists ---------------------------------------------------------
//
// A device command can change what a node collects (set_reports, TEC-NATKIT-40),
// and Parquet has ONE COLUMN SET PER FILE. Changing the sensor configuration
// mid-recording therefore changes a file's schema halfway through, which nothing
// downstream can express -- so Zach's decision is to refuse the change rather
// than to keep every column and mark the disabled spans, or to roll a new file.
//
// ⚠️ THE CHECK BELONGS ON THE SERVER, NOT THE DEVICE. A leaf cannot know whether
// anything is recording it, and a check in the browser is a suggestion rather
// than a guarantee -- the same command can be published straight to the broker.
// The one place that knows and that everything must pass through is here.
//
// ⚠️ AND IT IS DELIBERATELY A FLAG RATHER THAN A HANDLE TO RecordingSession.
// The recording lives in NatKitBackend.cpp behind a mutex that the command path
// has no business taking: a websocket handler blocking on the recording's lock
// while a sample is being appended would couple the command channel to the data
// path, which is the kind of thing that shows up later as unexplained latency.
// One atomic bool answers the only question the command path actually has.

// Set by the recording lifecycle in NatKitBackend.
void setRecordingActive(bool active);

// Read by anything that must not act while a recording is running.
bool isRecordingActive();

}  // namespace tools
}  // namespace nat
