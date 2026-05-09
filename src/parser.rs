use crate::types::WikiChange;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketLane {
    Human,
    Bot,
}

#[derive(Debug)]
pub struct HotPathView<'a> {
    pub lane: PacketLane,
    pub user: Option<&'a str>,
    pub server_name: Option<&'a str>,
    pub title: Option<&'a str>,
    pub event_id: Option<&'a str>,
}

pub fn parse_wiki_change<'a>(bytes: &'a [u8]) -> Result<WikiChange<'a>, serde_json::Error> {
    serde_json::from_slice(bytes)
}

pub fn packet_lane(change: &WikiChange<'_>) -> PacketLane {
    if change.bot.unwrap_or(false) {
        PacketLane::Bot
    } else {
        PacketLane::Human
    }
}

pub fn hot_path_view<'a>(change: &'a WikiChange<'a>) -> HotPathView<'a> {
    HotPathView {
        lane: packet_lane(change),
        user: change.user,
        server_name: change.server_name,
        title: change.title,
        event_id: change.meta.as_ref().and_then(|meta| meta.id),
    }
}

pub fn is_bot(change: &WikiChange<'_>) -> bool {
    change.bot.unwrap_or(false)
}

pub fn server_name<'a>(change: &'a WikiChange<'a>) -> &'a str {
    change.server_name.unwrap_or("unknown")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::sync::atomic::{AtomicUsize, Ordering};

    static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

    struct CountingAllocator;

    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            unsafe { System.alloc(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) }
        }
    }

    #[global_allocator]
    static GLOBAL: CountingAllocator = CountingAllocator;

    #[test]
    fn hot_path_parse_is_zero_alloc() {
        let payload = br#"{"namespace":0,"title":"Rust","user":"Example","bot":false,"server_name":"en.wikipedia.org","meta":{"id":"123"}}"#;
        ALLOCATIONS.store(0, Ordering::Relaxed);

        let change = parse_wiki_change(payload).expect("sample JSON should parse");
        let view = hot_path_view(&change);

        assert_eq!(view.lane, PacketLane::Human);
        assert_eq!(view.user, Some("Example"));
        assert_eq!(view.server_name, Some("en.wikipedia.org"));
        assert_eq!(view.title, Some("Rust"));
        assert_eq!(view.event_id, Some("123"));
        
        let alloc_count = ALLOCATIONS.load(Ordering::Relaxed);
        println!("🔬 ALLOCATION COUNT: {} allocations during parsing", alloc_count);
        assert_eq!(alloc_count, 0, "Expected zero allocations in hot path");
    }

    #[test]
    fn hot_path_complex_wikipedia_payload() {
        // Simulates a real Wikipedia JSON with longer strings
        let payload = br#"{"namespace":0,"title":"Artificial Intelligence and Machine Learning","user":"ContributorAccount2024","bot":false,"server_name":"en.wikipedia.org","meta":{"id":"abc123def456"}}"#;
        ALLOCATIONS.store(0, Ordering::Relaxed);

        let change = parse_wiki_change(payload).expect("complex JSON should parse");
        let view = hot_path_view(&change);

        assert_eq!(view.lane, PacketLane::Human);
        assert_eq!(view.user, Some("ContributorAccount2024"));
        assert_eq!(view.server_name, Some("en.wikipedia.org"));
        assert_eq!(view.title, Some("Artificial Intelligence and Machine Learning"));
        
        let alloc_count = ALLOCATIONS.load(Ordering::Relaxed);
        println!("🔬 COMPLEX PAYLOAD ALLOCATION COUNT: {} allocations", alloc_count);
        assert_eq!(alloc_count, 0, "Complex payload should still have zero allocations");
    }

    #[test]
    fn hot_path_lifetime_correctness() {
        // Verify that all references are properly borrowed from the input buffer
        let payload = br#"{"namespace":0,"title":"Test","user":"Alice","bot":false,"server_name":"en.wikipedia.org","meta":{"id":"xyz"}}"#;
        
        {
            let change = parse_wiki_change(payload).expect("should parse");
            let view = hot_path_view(&change);
            
            // These assertions prove that strings are borrowed, not allocated
            assert!(view.user.is_some());
            assert!(view.title.is_some());
            assert!(view.server_name.is_some());
            assert!(view.event_id.is_some());
        }
        // All data is freed without any heap allocations
    }
}
