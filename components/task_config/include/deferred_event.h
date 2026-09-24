#pragma once

#include <mutex>
#include <utility>
#include <vector>

// deferred_event::Scope locks a service's state mutex and delivers any events posted while it
// is held only AFTER the mutex is released.
//
// Why: services used to call their event handler while still holding their own mutex. When two
// services' handlers call into each other (transcription -> session, session -> transcription
// GetSnapshot), that is a lock-order inversion that can deadlock, and any slow handler work
// (SD writes, UI updates) blocks every GetSnapshot() caller for its duration.
//
// Usage, inside a service:
//   using EventScope = deferred_event::Scope<Event>;
//   EventScope lock(s_mutex);                             // instead of std::lock_guard
//   EventScope::Post(s_event_handler, s_event_context, {.snapshot = s_snapshot});
//
// Post() must be called with the mutex held through a Scope. Events posted in one scope are
// delivered in order, on the same thread, before the Scope's destructor returns, so callers
// still observe synchronous delivery; only the lock is gone by then.
//
// The active scope is tracked in a static that is only read or written while the mutex is held,
// so no thread-local storage is needed. Each service instantiates the template with its own
// Event type, so each service gets its own static.
namespace deferred_event {

template <typename Event>
class Scope {
public:
    using Handler = void (*)(const Event& event, void* context);

    explicit Scope(std::mutex& mutex) : lock_(mutex)
    {
        active_ = this;
    }

    ~Scope()
    {
        active_ = nullptr;
        std::vector<Pending> pending = std::move(pending_);
        lock_.unlock();
        for (const Pending& item : pending) {
            item.handler(item.event, item.context);
        }
    }

    Scope(const Scope&) = delete;
    Scope& operator=(const Scope&) = delete;

    // Queues an event for delivery when the active scope releases the mutex. If no scope is
    // active (a programming error: the caller does not hold the mutex through a Scope), the
    // event is delivered immediately rather than dropped.
    static void Post(Handler handler, void* context, Event event)
    {
        if (handler == nullptr) {
            return;
        }
        Scope* scope = active_;
        if (scope == nullptr) {
            handler(event, context);
            return;
        }
        scope->pending_.push_back(Pending{handler, context, std::move(event)});
    }

private:
    struct Pending {
        Handler handler;
        void* context;
        Event event;
    };

    std::unique_lock<std::mutex> lock_;
    std::vector<Pending> pending_;

    static inline Scope* active_ = nullptr;
};

}  // namespace deferred_event
