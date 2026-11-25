// What runs when, with how much. No I/O here: the daemon holds a Queue behind its lock,
// asks plan() what to start, and reports back with finished().
//
// A submission is a chain of groups: `A -p B -q C` is [[A, B], [C]]. A group's jobs run
// side by side; the next group starts once the whole group is done. The main lane runs
// one chain at a time, in submission order. A submission that starts with -p goes in the
// express lane instead: it starts as soon as resources allow, alongside everything else.
use std::{
    collections::VecDeque,
    path::PathBuf,
    time::SystemTime,
};
use crate::cli::Cmd;
use crate::server::jobs::{Job, JobId};

pub type ChainId = u64;

#[derive(Clone, Copy, PartialEq, Debug, Default)]
pub struct Res {
    pub ram: usize,
    pub workers: usize,
}

pub struct Limits {
    pub total: Res,
    pub max_jobs: usize,
    pub min: Res,
    pub max: Res,
    /// express jobs that don't fit start with what's free instead of waiting
    pub shrink_express: bool,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Lane {
    Main,
    Express,
}

pub struct Chain {
    pub id: ChainId,
    pub lane: Lane,
    /// The front group stays here (possibly emptied) until its running jobs finish.
    pub groups: VecDeque<Vec<Job>>,
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum StopRequest {
    Drop,
    Pause,
    /// the server is stopping: put it back in the queue to resume on the next start
    Requeue,
}

pub struct Running {
    pub job: Job,
    pub chain: ChainId,
    pub alloc: Res,
    pub log: PathBuf,
    pub started: SystemTime,
    pub stop: Option<StopRequest>,
}

/// A job plan() decided to start.
pub struct Start {
    pub job: Job,
    pub chain: ChainId,
    pub alloc: Res,
}

#[derive(Default)]
pub struct Queue {
    pub main: VecDeque<Chain>,
    pub express: Vec<Chain>,
    pub paused: Vec<Job>,
    pub running: Vec<Running>,
}

/// Where a waiting job is, for `ls`.
pub enum Spot {
    /// main lane, nth group to run (1-based)
    Queued(usize),
    Express,
    Paused,
}

impl Queue {
    pub fn submit(&mut self, chain: Chain) {
        match chain.lane {
            Lane::Main => self.main.push_back(chain),
            Lane::Express => self.express.push(chain),
        }
    }

    /// Picks the jobs to start now and takes them out of their groups. The caller must
    /// add a Running entry for each before releasing the lock.
    pub fn plan(&mut self, lim: &Limits) -> Vec<Start> {
        self.cleanup();
        let used = self.running.iter().fold(Res::default(), |a, r| Res {
            ram: a.ram + r.alloc.ram,
            workers: a.workers + r.alloc.workers,
        });
        let mut free = Res {
            ram: lim.total.ram.saturating_sub(used.ram),
            workers: lim.total.workers.saturating_sub(used.workers),
        };
        let mut slots = lim.max_jobs.saturating_sub(self.running.len());

        // express first (they jump the queue), then the head of the main lane
        let mut cands: Vec<(ChainId, JobId, Lane)> = Vec::new();
        for c in self.express.iter().chain(self.main.front()) {
            if let Some(g) = c.groups.front() {
                cands.extend(g.iter().map(|j| (c.id, j.id, c.lane)));
            }
        }

        let mut starts = Vec::new();
        for (chain, id, lane) in cands {
            if slots == 0 {
                break;
            }
            let job = self.chain_mut(chain).unwrap().groups[0].iter().find(|j| j.id == id).unwrap();
            let shrink = lane == Lane::Express && lim.shrink_express;
            // strict order: a job that doesn't fit holds back everything behind it
            // auto-sized jobs split what's free across the open job slots, not just the
            // jobs waiting now, so a later job (or an express one) still finds room
            let Some(alloc) = allocate(job, free, slots, lim, shrink) else { break };
            let g = &mut self.chain_mut(chain).unwrap().groups[0];
            let pos = g.iter().position(|j| j.id == id).unwrap();
            let job = g.remove(pos);
            free.ram -= alloc.ram;
            free.workers -= alloc.workers;
            slots -= 1;
            starts.push(Start { job, chain, alloc });
        }
        starts
    }

    /// Takes a finished job out of `running`. If it failed, the rest of its chain is
    /// dropped (jobs already started or alongside it in its group are left alone);
    /// those jobs are returned.
    pub fn finished(&mut self, id: JobId, failed: bool) -> (Option<Running>, Vec<Job>) {
        let Some(pos) = self.running.iter().position(|r| r.job.id == id) else {
            return (None, Vec::new());
        };
        let r = self.running.remove(pos);
        let mut skipped = Vec::new();
        if failed {
            if let Some(c) = self.chain_mut(r.chain) {
                skipped = c.groups.drain(1..).flatten().collect();
            }
        }
        self.cleanup();
        (Some(r), skipped)
    }

    /// Puts a job that was stopped mid-run back where it was, to run again with -c.
    pub fn put_back(&mut self, mut job: Job, chain: ChainId) {
        job.args.resume = true;
        match self.chain_mut(chain) {
            Some(c) => match c.groups.front_mut() {
                // several jobs of a group come back one by one: keep them in id order
                Some(g) => {
                    let at = g.iter().position(|j| j.id > job.id).unwrap_or(g.len());
                    g.insert(at, job)
                }
                None => c.groups.push_front(vec![job]),
            },
            None => self.main.push_front(Chain { id: chain, lane: Lane::Main, groups: VecDeque::from([vec![job]]) }),
        }
    }

    /// Removes a job that hasn't started (queued, express or paused).
    pub fn remove_waiting(&mut self, id: JobId) -> Option<Job> {
        if let Some(p) = self.paused.iter().position(|j| j.id == id) {
            return Some(self.paused.remove(p));
        }
        for c in self.main.iter_mut().chain(self.express.iter_mut()) {
            for g in c.groups.iter_mut() {
                if let Some(p) = g.iter().position(|j| j.id == id) {
                    let j = g.remove(p);
                    self.cleanup();
                    return Some(j);
                }
            }
        }
        None
    }

    pub fn waiting_mut(&mut self, id: JobId) -> Option<&mut Job> {
        self.paused.iter_mut()
            .chain(self.main.iter_mut().chain(self.express.iter_mut()).flat_map(|c| c.groups.iter_mut().flatten()))
            .find(|j| j.id == id)
    }

    /// A paused job goes to the express lane on its own, so it continues right away.
    pub fn resume(&mut self, id: JobId) -> bool {
        let Some(p) = self.paused.iter().position(|j| j.id == id) else { return false };
        let job = self.paused.remove(p);
        self.express.push(Chain { id: job.id, lane: Lane::Express, groups: VecDeque::from([vec![job]]) });
        true
    }

    /// Every job that hasn't started, in run order, with where it is.
    pub fn waiting(&self) -> Vec<(&Job, Spot)> {
        let mut v = Vec::new();
        let mut n = 0;
        for c in &self.main {
            for g in &c.groups {
                if g.is_empty() { continue; }
                n += 1;
                v.extend(g.iter().map(|j| (j, Spot::Queued(n))));
            }
        }
        for c in &self.express {
            v.extend(c.groups.iter().flatten().map(|j| (j, Spot::Express)));
        }
        v.extend(self.paused.iter().map(|j| (j, Spot::Paused)));
        v
    }

    /// Everything that would need to run again after a restart, as
    /// (job, chain, group index within the chain, lane or None for paused).
    pub fn saved(&self) -> Vec<(&Job, ChainId, usize, Option<Lane>)> {
        let mut v = Vec::new();
        for c in self.main.iter().chain(self.express.iter()) {
            for (gi, g) in c.groups.iter().enumerate() {
                v.extend(g.iter().map(|j| (j, c.id, gi, Some(c.lane))));
            }
        }
        v.extend(self.paused.iter().map(|j| (j, j.id, 0, None)));
        v
    }

    /// Rebuilds the queue from `saved()` output, in the same order.
    pub fn restore(entries: Vec<(Job, ChainId, usize, Option<Lane>)>) -> Self {
        let mut q = Queue::default();
        for (job, chain, gi, lane) in entries {
            let Some(lane) = lane else {
                q.paused.push(job);
                continue;
            };
            if q.chain_mut(chain).is_none() {
                q.submit(Chain { id: chain, lane, groups: VecDeque::new() });
            }
            let c = q.chain_mut(chain).unwrap();
            while c.groups.len() <= gi {
                c.groups.push_back(Vec::new());
            }
            c.groups[gi].push(job);
        }
        q.cleanup();
        q
    }

    fn chain_mut(&mut self, id: ChainId) -> Option<&mut Chain> {
        self.main.iter_mut().chain(self.express.iter_mut()).find(|c| c.id == id)
    }

    /// Moves chains on to their next group once the current one is done, and forgets
    /// chains that have nothing left.
    fn cleanup(&mut self) {
        let running = &self.running;
        let busy = |c: &Chain| running.iter().any(|r| r.chain == c.id);
        for c in self.main.iter_mut().chain(self.express.iter_mut()) {
            while c.groups.front().is_some_and(|g| g.is_empty()) && !busy(c) {
                c.groups.pop_front();
            }
        }
        self.main.retain(|c| !c.groups.is_empty() || busy(c));
        self.express.retain(|c| !c.groups.is_empty() || busy(c));
    }
}

/// RAM and workers for one job, or None if it has to wait. Jobs without -r/-n get
/// 1/`share` of what's free, within the configured bounds. Explicit -r/-n is honored up to the
/// server's total.
pub fn allocate(job: &Job, free: Res, share: usize, lim: &Limits, shrink: bool) -> Option<Res> {
    let a = &job.args;
    let want = if a.cmd == Cmd::Extract {
        // extract ignores -r/-n: one reader, one writer
        Res { ram: lim.min.ram, workers: 1 }
    } else {
        let share = share.max(1);
        Res {
            ram: if a.ram_given { a.max_ram.min(lim.total.ram) } else { (free.ram / share).clamp(lim.min.ram, lim.max.ram) },
            workers: if a.workers_given { a.num_workers.min(lim.total.workers) } else { (free.workers / share).clamp(lim.min.workers, lim.max.workers) },
        }
    };
    if want.ram <= free.ram && want.workers <= free.workers {
        Some(want)
    } else if shrink && free.ram >= lim.min.ram && free.workers >= 1 {
        Some(Res { ram: want.ram.min(free.ram), workers: want.workers.min(free.workers) })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::parse_args_from;

    const MB: usize = 1 << 20;

    fn job(id: JobId, extra: &[&str]) -> Job {
        let mut argv: Vec<std::ffi::OsString> = vec!["/".into(), "/".into()];
        argv.extend(extra.iter().map(|s| s.into()));
        Job { id, args: parse_args_from(argv).unwrap(), submitted: SystemTime::now() }
    }

    fn lim() -> Limits {
        Limits {
            total: Res { ram: 100 * MB, workers: 8 },
            max_jobs: 4,
            min: Res { ram: 10 * MB, workers: 1 },
            max: Res { ram: 40 * MB, workers: 4 },
            shrink_express: false,
        }
    }

    fn chain(id: ChainId, lane: Lane, groups: Vec<Vec<Job>>) -> Chain {
        Chain { id, lane, groups: groups.into() }
    }

    /// Runs plan() and records the starts as running, like the daemon does.
    fn start(q: &mut Queue, l: &Limits) -> Vec<JobId> {
        let s = q.plan(l);
        let ids = s.iter().map(|s| s.job.id).collect();
        for s in s {
            q.running.push(Running { job: s.job, chain: s.chain, alloc: s.alloc, log: PathBuf::new(), started: SystemTime::now(), stop: None });
        }
        ids
    }

    #[test]
    fn groups_run_in_order_and_chains_fifo() {
        let mut q = Queue::default();
        q.submit(chain(1, Lane::Main, vec![vec![job(1, &[]), job(2, &[])], vec![job(3, &[])]]));
        q.submit(chain(4, Lane::Main, vec![vec![job(4, &[])]]));
        let l = lim();
        assert_eq!(start(&mut q, &l), vec![1, 2]);
        assert_eq!(start(&mut q, &l), Vec::<JobId>::new());
        q.finished(1, false);
        assert_eq!(start(&mut q, &l), Vec::<JobId>::new()); // 2 still running
        q.finished(2, false);
        assert_eq!(start(&mut q, &l), vec![3]);
        q.finished(3, false);
        assert_eq!(start(&mut q, &l), vec![4]);
    }

    #[test]
    fn express_jumps_the_queue() {
        let mut q = Queue::default();
        let l = lim();
        q.submit(chain(1, Lane::Main, vec![vec![job(1, &[])]]));
        assert_eq!(start(&mut q, &l), vec![1]);
        q.submit(chain(2, Lane::Main, vec![vec![job(2, &[])]]));
        q.submit(chain(3, Lane::Express, vec![vec![job(3, &[])]]));
        assert_eq!(start(&mut q, &l), vec![3]);
    }

    #[test]
    fn failure_skips_rest_of_chain() {
        let mut q = Queue::default();
        let l = lim();
        q.submit(chain(1, Lane::Main, vec![vec![job(1, &[])], vec![job(2, &[])], vec![job(3, &[])]]));
        assert_eq!(start(&mut q, &l), vec![1]);
        let (_, skipped) = q.finished(1, true);
        assert_eq!(skipped.iter().map(|j| j.id).collect::<Vec<_>>(), vec![2, 3]);
        assert!(q.main.is_empty());
    }

    #[test]
    fn explicit_requests_wait_for_room() {
        let mut q = Queue::default();
        let l = lim();
        q.submit(chain(1, Lane::Main, vec![vec![job(1, &["-r", "70MB"]), job(2, &["-r", "50MB"])]]));
        assert_eq!(start(&mut q, &l), vec![1]); // 2 doesn't fit next to 1
        q.finished(1, false);
        assert_eq!(start(&mut q, &l), vec![2]);
        // larger than the server total: clamped, not stuck forever
        q.finished(2, false);
        q.submit(chain(3, Lane::Main, vec![vec![job(3, &["-r", "1GB", "-n", "64"])]]));
        let s = q.plan(&l);
        assert_eq!(s[0].alloc, Res { ram: 100 * MB, workers: 8 });
    }

    #[test]
    fn auto_jobs_share_what_is_free() {
        let mut q = Queue::default();
        let l = lim();
        q.submit(chain(1, Lane::Main, vec![vec![job(1, &[]), job(2, &[]), job(3, &[])]]));
        let s = q.plan(&l);
        // a quarter of what's free each (4 job slots), so a fourth job still fits
        assert_eq!(s.len(), 3);
        assert_eq!(s[0].alloc, Res { ram: 25 * MB, workers: 2 });
        assert!(s.iter().map(|s| s.alloc.ram).sum::<usize>() <= 100 * MB - 10 * MB);
        assert!(s.iter().map(|s| s.alloc.workers).sum::<usize>() < 8);
        let mut q = Queue::default();
        q.submit(chain(1, Lane::Main, vec![vec![job(1, &[])]]));
        let mut l1 = lim();
        l1.max_jobs = 1;
        assert_eq!(q.plan(&l1)[0].alloc, Res { ram: 40 * MB, workers: 4 }); // capped at jobs.max
    }

    #[test]
    fn max_jobs_and_shrink() {
        let mut q = Queue::default();
        let mut l = lim();
        l.max_jobs = 1;
        q.submit(chain(1, Lane::Main, vec![vec![job(1, &["-r", "90MB"]), job(2, &[])]]));
        assert_eq!(start(&mut q, &l), vec![1]);
        l.max_jobs = 4;
        q.submit(chain(3, Lane::Express, vec![vec![job(3, &["-r", "50MB"])]]));
        assert_eq!(start(&mut q, &l), Vec::<JobId>::new()); // waits: only 10MB free
        l.shrink_express = true;
        let s = q.plan(&l);
        assert_eq!((s[0].job.id, s[0].alloc.ram), (3, 10 * MB));
    }

    #[test]
    fn pause_resume_and_restore() {
        let mut q = Queue::default();
        let l = lim();
        q.submit(chain(1, Lane::Main, vec![vec![job(1, &[])], vec![job(2, &[])]]));
        q.paused.push(job(9, &[]));
        let saved: Vec<_> = q.saved().into_iter().map(|(j, c, g, l)| (j.clone(), c, g, l)).collect();
        let mut r = Queue::restore(saved);
        assert_eq!(r.waiting().len(), 3);
        assert!(r.resume(9));
        assert_eq!(start(&mut r, &l), vec![9, 1]);
        // stopped mid-run: back at the front, with -c
        let run = r.running.remove(1);
        r.put_back(run.job, run.chain);
        assert!(r.main[0].groups[0][0].args.resume);
    }
}
