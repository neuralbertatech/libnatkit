#pragma once

// IS THIS NODE HEALTHY? (TEC-NATKIT-116)
//
// A graph node's runtime state was decided by one question — "has anything come
// out of it lately" — and that question is wrong for most of the operators this
// runtime has. Every operator whose output is EVENT-DRIVEN rather than one out
// per one in stamps its heartbeat on the emit path only, so on a healthy stream
// it emits nothing, looks dead, and (because `derived_run_state` is "any stalled
// node → the whole board is stalled") marks an entire healthy graph unhealthy.
//
// A gap detector is the pure case: its whole purpose is to speak up when data
// DROPS OUT, so it is silent exactly when everything is fine. The node whose job
// is to report faults looked broken precisely when there were none.
//
// The fix is to ask two questions instead of one:
//
//   is it being FED?     the newest wall-clock stamp across its input lanes
//   is it EMITTING?      the wall-clock stamp on its emit path
//
// ⚠️ ASKING ONLY THE FIRST, OR EXCUSING A KIND OUTRIGHT, IS THE SAME BUG
// MIRRORED. "Skip the heartbeat test for gap detectors" reports `live` forever
// for one whose upstream has died — a node at its most confident when it is most
// wrong, which is exactly what TEC-NATKIT-123 was. Not being fed is a stall for
// every kind, because it is true for every kind.
//
// ⚠️ WALL CLOCK, and deliberately the only place in the graph that is. Every
// temporal decision in the DATA path reads device_ts_us so a replay draws
// identically to the live run it recorded (docs/STREAM_CONTRACT.md clause 4).
// Liveness is the one question genuinely about the transport rather than the
// data — the same boundary that closed TEC-NATKIT-110 and kept the clock node
// out of the graph. It belongs in the status layer and nowhere else.
//
// Pure and now-injected so it can be tested without a clock, a worker or a
// stack: every input is a number.

#include <cstdint>
#include <string>

namespace nat::tools {

// How long a node may say nothing before it is called stalled.
constexpr uint64_t kWorkerStallUs = 3'000'000ULL;

// Kinds whose SILENCE IS NORMAL: their output is event-driven, so "nothing
// emitted for 3 s" is the healthy case rather than a fault.
//
// ⚠️ NOT A GAP_DETECT SPECIAL CASE — seven kinds share the shape. A threshold
// whose level sits above its signal, a gate that never opens and a filter that
// matches nothing are all silent while perfectly healthy; `gap_detect` was
// merely the one silent on a healthy board BY DESIGN, so it was the one somebody
// noticed first.
//
// `transform` and `combine` are deliberately absent: they owe an output for what
// they are fed, so their silence IS a fault worth reporting — including the
// starved-input case a combine's watermark holds on, which is precisely what the
// marble strips exist to make visible.
//
// ⚠️ Keep in step with the backend's own kind predicates. A kind added to the
// marker-operator family belongs here too, or it inherits the original bug.
inline bool silenceIsNormalKind(const std::string& kind)
{
    return kind == "gap_detect" || kind == "threshold" || kind == "gate" ||
        kind == "marker_merge" || kind == "marker_filter" ||
        kind == "marker_debounce" || kind == "marker_take_until";
}

// What a node's status should read, given when it last emitted, when it was last
// fed, and when it started. All three are wall-clock microseconds; 0 means
// "never".
//
// `has_input_signal` is false for a node with no input lanes reporting at all —
// a plain transform, or an older worker that predates per-input activity
// (TEC-NATKIT-119). Such a node can only be judged on its output, which is the
// behaviour this had before, so it keeps it.
inline std::string classifyGraphNodeStatusAt(
    uint64_t now_wall_us,
    uint64_t last_output_at_wall_us,
    uint64_t last_input_at_wall_us,
    uint64_t started_at_wall_us,
    bool silence_is_normal,
    bool has_input_signal)
{
    const auto age = [now_wall_us](uint64_t at_us) -> uint64_t {
        return now_wall_us > at_us ? now_wall_us - at_us : 0;
    };

    // A worker that has not yet had time to say anything is STARTING, not
    // stalled. Without this every node reads stalled for the first three seconds
    // of every run, which is how a state earns being ignored.
    const bool never_emitted = last_output_at_wall_us == 0;
    const bool never_fed = !has_input_signal || last_input_at_wall_us == 0;
    if (never_emitted && never_fed && started_at_wall_us != 0 &&
        age(started_at_wall_us) < kWorkerStallUs) {
        return "starting";
    }

    // Owes an output for what it is fed, or has no input lane to be judged on.
    if (!silence_is_normal || !has_input_signal) {
        return age(last_output_at_wall_us) >= kWorkerStallUs ? "stalled" : "live";
    }

    // Emitting recently is enough on its own: a marker operator can legitimately
    // out-live a burst of input it is still draining.
    if (age(last_output_at_wall_us) < kWorkerStallUs) {
        return "live";
    }

    // Silent — which is only excusable while something is still arriving.
    return age(last_input_at_wall_us) >= kWorkerStallUs ? "stalled" : "live";
}

}  // namespace nat::tools
