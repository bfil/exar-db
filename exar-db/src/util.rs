use stopwatch::Stopwatch;

pub fn report_performance(sw: Stopwatch, num_events: usize, label: &str) {
    let timing = sw.elapsed_ms() as u64;
    let num_events = num_events as u64;
    println!("{} {} events took {}ms..", label, num_events, timing);
    if timing != 0 {
        println!("{} performance was {} events per second..", label, num_events * 1000 / timing);
    } else {
        println!("{} performance was not possible to calculate..", label);
    }
}
