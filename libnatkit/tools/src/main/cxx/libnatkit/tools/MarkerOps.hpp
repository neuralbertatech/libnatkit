#pragma once

// The MARKER-LANE ALGEBRA (TEC-NATKIT-105).
//
// The marker lane is the part of the graph that is genuinely Rx-shaped —
// unclocked events arriving at unknown times in unknown quantities — and until
// now it had almost no operators. `combine` could interleave marker inputs and a
// `markers` node could produce them; nothing could act on them.
//
// These are the four FLAT operators, deliberately. Rx's higher-order family
// (flatMap, switchMap, window-as-observable-of-observables) builds topology at
// run time, which a fixed node graph cannot draw, and that is where visual-Rx
// tools reliably become unreadable. Each operator here has an obvious picture on
// a node card.
//
// ⚠️ EVERY DECISION READS THE MARKER'S OWN emitted_at_us, NEVER ARRIVAL TIME
// (docs/STREAM_CONTRACT.md clause 4). `debounce` is the tempting one to
// implement against the wall clock — doing so makes a 1x replay disagree with a
// max-speed replay of the same recording, and would pass every test that ran in
// real time.
//
// Transport-free by design, like CombineJoin.hpp: the worker owns the I/O.

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "TimeOrderedMerge.hpp"

namespace nat::tools {

// The fields of a MarkerEventV1 an operator may match on. Kept to the three that
// identify an event; matching on the session id or the opaque attributes JSON
// would be matching on provenance rather than on what happened.
enum class MarkerMatchField {
    Label,
    Event,
    MarkerType,
};

inline const char* markerMatchFieldName(MarkerMatchField field)
{
    switch (field) {
    case MarkerMatchField::Label:
        return "label";
    case MarkerMatchField::Event:
        return "event";
    case MarkerMatchField::MarkerType:
        return "marker_type";
    }
    return "label";
}

inline std::optional<MarkerMatchField> parseMarkerMatchField(
    const std::string& name)
{
    if (name == "label") return MarkerMatchField::Label;
    if (name == "event") return MarkerMatchField::Event;
    if (name == "marker_type") return MarkerMatchField::MarkerType;
    return std::nullopt;
}

// Everything an operator needs to know about one marker.
struct MarkerEventView {
    uint64_t atUs = 0;
    std::string label{};
    std::string event{};
    std::string markerType{};

    const std::string& field(MarkerMatchField which) const
    {
        switch (which) {
        case MarkerMatchField::Label:
            return label;
        case MarkerMatchField::Event:
            return event;
        case MarkerMatchField::MarkerType:
            return markerType;
        }
        return label;
    }
};

// Splits a comma-separated match list, trimming surrounding whitespace and
// discarding empty entries. Comma-separated because a list field is not in the
// catalog's config-field vocabulary yet; trimming because "fist, pinch" is what
// a person types and refusing it would be pedantry that reads as a bug.
inline std::vector<std::string> parseMarkerMatchValues(const std::string& raw)
{
    std::vector<std::string> values;
    std::string current;
    const auto flush = [&]() {
        const auto begin = current.find_first_not_of(" \t");
        if (begin != std::string::npos) {
            const auto end = current.find_last_not_of(" \t");
            values.push_back(current.substr(begin, end - begin + 1));
        }
        current.clear();
    };
    for (const char character : raw) {
        if (character == ',') {
            flush();
        } else {
            current.push_back(character);
        }
    }
    flush();
    return values;
}

// --- filter ---------------------------------------------------------------

struct MarkerFilterConfig {
    MarkerMatchField field = MarkerMatchField::Label;
    // Matched by exact equality against the chosen field. Several values are
    // ORed, which is what "only the cue onsets" usually needs.
    std::vector<std::string> values{};
    // Exclude inverts the match. Both directions are wanted in practice: "only
    // these cues" and "everything except the rest periods".
    bool exclude = false;
};

// Stateless: a filter's answer depends on the marker alone, so it needs no
// watermark and no ordering guarantee.
class MarkerFilter {
public:
    explicit MarkerFilter(MarkerFilterConfig config) : config_(std::move(config)) {}

    bool passes(const MarkerEventView& marker) const
    {
        // An empty value list is a pass-through rather than a block. A filter
        // that silently dropped everything until configured would look exactly
        // like a broken upstream, which is the worst failure mode available on
        // this lane.
        if (config_.values.empty()) {
            return true;
        }
        const auto& actual = marker.field(config_.field);
        bool matched = false;
        for (const auto& wanted : config_.values) {
            if (actual == wanted) {
                matched = true;
                break;
            }
        }
        return config_.exclude ? !matched : matched;
    }

private:
    MarkerFilterConfig config_{};
};

// --- debounce -------------------------------------------------------------

// Suppresses markers arriving within `windowUs` of the last one PASSED (not of
// the last one seen — otherwise a dense burst would extend the suppression
// indefinitely and the operator would pass exactly one marker for ever).
//
// This is the operator that makes a physical button or a jittery cue source
// usable as a graph input at all. Single-lane, so arrival order is the canonical
// order per the stream contract and no watermark is needed.
class MarkerDebounce {
public:
    explicit MarkerDebounce(uint64_t window_us) : windowUs_(window_us) {}

    bool admit(uint64_t at_us)
    {
        if (windowUs_ == 0) {
            return true;  // a zero window is a pass-through, not a block
        }
        if (passedAny_ && at_us < lastPassedUs_ + windowUs_) {
            ++suppressed_;
            return false;
        }
        lastPassedUs_ = at_us;
        passedAny_ = true;
        return true;
    }

    uint64_t suppressed() const { return suppressed_; }

private:
    uint64_t windowUs_ = 0;
    uint64_t lastPassedUs_ = 0;
    bool passedAny_ = false;
    uint64_t suppressed_ = 0;
};

// --- merge ----------------------------------------------------------------

// N marker lanes into one, ordered by emitted_at_us. `combine`'s marker lane
// already interleaves by ARRIVAL; this orders by the markers' own time, which is
// the difference between a merged timeline you can reason about and one that
// reflects network scheduling.
template <typename Marker>
class MarkerMerge {
public:
    explicit MarkerMerge(size_t lane_count, size_t max_queued_per_lane = 256)
        : merge_(lane_count, max_queued_per_lane)
    {
    }

    void push(size_t lane, Marker marker, uint64_t at_us)
    {
        merge_.push(lane, std::move(marker), at_us);
    }

    std::optional<Marker> tryEmit()
    {
        auto next = merge_.next();
        if (!next.has_value()) {
            return std::nullopt;
        }
        return std::move(next->second);
    }

    uint64_t droppedOverflow() const { return merge_.droppedOverflow(); }

private:
    TimeOrderedMerge<Marker> merge_;
};

// --- take-until -----------------------------------------------------------

// Passes markers from the primary lane until a marker arrives on the stop lane,
// then stops for good (Rx's takeUntil completes the stream; a live channel here
// simply goes quiet).
//
// ⚠️ "UNTIL" MEANS BY TIMESTAMP, NOT BY ARRIVAL. This is the question the ticket
// left open, and the merge answers it: a marker timestamped before the stop
// marker passes even if it arrived afterwards. Deciding on arrival would make
// the boundary depend on network scheduling, so the same recording replayed
// would cut in a different place.
template <typename Marker>
class MarkerTakeUntil {
public:
    static constexpr size_t kPrimaryLane = 0;
    static constexpr size_t kStopLane = 1;

    explicit MarkerTakeUntil(size_t max_queued_per_lane = 256)
        : merge_(2, max_queued_per_lane)
    {
    }

    void pushPrimary(Marker marker, uint64_t at_us)
    {
        merge_.push(kPrimaryLane, std::move(marker), at_us);
    }

    void pushStop(Marker marker, uint64_t at_us)
    {
        merge_.push(kStopLane, std::move(marker), at_us);
    }

    std::optional<Marker> tryEmit()
    {
        while (auto next = merge_.next()) {
            if (next->first == kStopLane) {
                stopped_ = true;
                continue;  // the stop marker itself is not forwarded
            }
            if (stopped_) {
                ++droppedAfterStop_;
                continue;
            }
            return std::move(next->second);
        }
        return std::nullopt;
    }

    bool stopped() const { return stopped_; }
    // Primary markers seen after the stop. A rising count is the honest signal
    // that the upstream is still producing into a closed operator.
    uint64_t droppedAfterStop() const { return droppedAfterStop_; }
    uint64_t droppedOverflow() const { return merge_.droppedOverflow(); }

private:
    TimeOrderedMerge<Marker> merge_;
    bool stopped_ = false;
    uint64_t droppedAfterStop_ = 0;
};

}  // namespace nat::tools
