use nalarvo_domain::*;

#[test]
fn hard_dependencies_gate_ready_and_cycles_reject() {
    let company = CompanyId::new();
    let project = "project".to_owned();
    let blocked = WorkItem::create(company.clone(), project.clone(), "blocked".into()).unwrap();
    let mut work = WorkItem::create(company, project, "work".into()).unwrap();
    assert!(work.ready(1, true).is_err());
    work.ready(1, false).unwrap();
    assert_eq!(work.status, WorkItemStatus::Ready);
    assert!(would_create_cycle(&[("a", "b"), ("b", "c")], "c", "a"));
    assert!(!would_create_cycle(&[("a", "b")], "b", "c"));
    assert_eq!(blocked.status, WorkItemStatus::Backlog);
}

#[test]
fn work_item_follows_the_canonical_lifecycle() {
    let mut work = WorkItem::create(CompanyId::new(), "project".into(), "work".into()).unwrap();

    work.ready(1, false).unwrap();
    work.start(2).unwrap();
    work.block(3).unwrap();
    assert_eq!(work.status, WorkItemStatus::Blocked);
    work.unblock(4).unwrap();
    assert_eq!(work.status, WorkItemStatus::InProgress);
    work.fail(5).unwrap();

    assert_eq!(work.status, WorkItemStatus::Failed);
    assert!(work.start(6).is_err());
    assert!(work.complete(6).is_err());
}

#[test]
fn work_item_only_blocks_in_progress_work() {
    let mut work = WorkItem::create(CompanyId::new(), "project".into(), "work".into()).unwrap();

    assert!(work.block(1).is_err());
    work.ready(1, false).unwrap();
    assert!(work.block(2).is_err());
    assert!(work.fail(2).is_err());
    work.start(2).unwrap();
    work.block(3).unwrap();
    assert!(work.ready(4, false).is_err());
}
