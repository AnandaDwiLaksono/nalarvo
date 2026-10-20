#![cfg(windows)]
use nalarvo_application::project_filesystem::ProjectFilesystem;
use nalarvo_runtime::native_tools::NativeTools;
use std::{fs, path::Path, process::Command};

fn junction(link: &Path, target: &Path) {
    let status = Command::new("cmd.exe")
        .args(["/d", "/c", "mklink", "/J"])
        .arg(link)
        .arg(target)
        .output()
        .unwrap();
    assert!(
        status.status.success(),
        "junction creation failed: {:?}",
        status
    );
}

#[test]
fn normal_read_list_write_and_windows_path_escape_matrix() {
    let root = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let tools = NativeTools::new(root.path(), [] as [&str; 0]).unwrap();
    fs::write(root.path().join("note.txt"), b"ok").unwrap();
    tools.write("new.txt", b"new").unwrap();
    tools.write("note.txt", b"ok").unwrap();
    assert_eq!(tools.read("new.txt").unwrap(), b"new");
    assert_eq!(tools.read("note.txt").unwrap(), b"ok");
    assert_eq!(tools.list("").unwrap(), ["new.txt", "note.txt"]);
    let absolute = other.path().join("escape.txt").to_str().unwrap().to_owned();
    for path in [
        "../escape",
        "sub/../../escape",
        "C:escape",
        "C:\\escape",
        "\\\\localhost\\C$\\escape",
        "\\\\?\\C:\\escape",
        "\\escape",
        "note.txt:stream",
        "NUL",
        "note.txt.",
        "note.txt ",
        &absolute,
    ] {
        assert!(tools.read(path).is_err(), "read: {path}");
        assert!(tools.list(path).is_err(), "list: {path}");
        assert!(tools.write(path, b"bad").is_err(), "write: {path}");
    }
    assert!(fs::read_dir(other.path()).unwrap().next().is_none());
}

#[test]
fn junction_dangling_reparse_and_hardlink_are_rejected() {
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("root");
    let outside = base.path().join("outside");
    fs::create_dir(&root).unwrap();
    fs::create_dir(&outside).unwrap();
    fs::write(outside.join("data"), b"outside").unwrap();
    junction(&root.join("junction"), &outside);
    junction(&root.join("dangling"), &base.path().join("missing"));
    fs::hard_link(outside.join("data"), root.join("hardlink")).unwrap();
    let tools = NativeTools::new(&root, [] as [&str; 0]).unwrap();
    for path in ["junction/data", "dangling/data", "hardlink"] {
        assert!(tools.read(path).is_err());
        assert!(tools.write(path, b"bad").is_err());
    }
    assert!(tools.list("junction").is_err());
    assert!(tools.list("dangling").is_err());
    assert!(ProjectFilesystem::open(&root.join("junction")).is_err());
    assert_eq!(fs::read(outside.join("data")).unwrap(), b"outside");
    drop(tools);
    fs::remove_dir(root.join("junction")).unwrap();
    fs::remove_dir(root.join("dangling")).unwrap();
}

#[test]
fn windows_symlink_and_dangling_symlink_are_rejected() {
    use std::os::windows::fs::{symlink_dir, symlink_file};
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    fs::write(outside.path().join("data"), b"outside").unwrap();
    // Do not silently skip: a host without symlink privilege must report the unmet gate.
    symlink_dir(outside.path(), root.path().join("link")).unwrap();
    symlink_file(outside.path().join("data"), root.path().join("file")).unwrap();
    symlink_file(outside.path().join("missing"), root.path().join("dangling")).unwrap();
    let tools = NativeTools::new(root.path(), [] as [&str; 0]).unwrap();
    for path in ["link/data", "file", "dangling"] {
        assert!(tools.read(path).is_err());
        assert!(tools.write(path, b"bad").is_err());
    }
    assert!(tools.list("link").is_err());
    assert_eq!(fs::read(outside.path().join("data")).unwrap(), b"outside");
}

#[test]
fn deterministic_parent_target_and_root_swap_cannot_redirect_overwrite() {
    let base = tempfile::tempdir().unwrap();
    let root = base.path().join("root");
    let outside = base.path().join("outside");
    fs::create_dir(&root).unwrap();
    fs::create_dir(&outside).unwrap();
    fs::create_dir(root.join("parent")).unwrap();
    fs::write(root.join("parent/data"), b"old").unwrap();
    fs::write(outside.join("data"), b"outside").unwrap();
    let filesystem = ProjectFilesystem::open(&root).unwrap();
    let pinned = filesystem.prepare("parent/data", true, false).unwrap();
    // Scoped thread join is the validation/operation barrier: no sleep, no probabilistic race.
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                assert!(fs::rename(&root, base.path().join("moved-root")).is_err());
                assert!(fs::rename(root.join("parent"), root.join("moved-parent")).is_err());
                // Target can move; publication rechecks material and never writes old inode.
                assert!(fs::write(root.join("parent/data"), b"attacker").is_err());
                // Windows permits alias creation despite the pinned handle. This is the
                // source alias is rejected at the publication recheck, not trusted as a lock.
                fs::hard_link(root.join("parent/data"), outside.join("alias")).unwrap();
            })
            .join()
            .unwrap();
    });
    assert!(filesystem.write(pinned, b"unauthorized").is_err());
    assert_eq!(fs::read(root.join("parent/data")).unwrap(), b"old");
    assert_eq!(fs::read(outside.join("alias")).unwrap(), b"old");
    assert_eq!(fs::read(outside.join("data")).unwrap(), b"outside");
}

#[test]
fn target_appearing_after_validation_fails_without_overwrite() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    fs::write(outside.path().join("data"), b"outside").unwrap();
    let filesystem = ProjectFilesystem::open(root.path()).unwrap();
    let pinned = filesystem.prepare("data", true, false).unwrap();
    // Populate an ordinary target between validation and operation; it must not be overwritten.
    fs::write(root.path().join("data"), b"appeared").unwrap();
    assert!(filesystem.write(pinned, b"bad").is_err());
    assert_eq!(fs::read(root.path().join("data")).unwrap(), b"appeared");
    assert_eq!(fs::read(outside.path().join("data")).unwrap(), b"outside");
}

#[test]
fn missing_invalid_and_reparse_project_roots_fail_closed() {
    let base = tempfile::tempdir().unwrap();
    fs::write(base.path().join("file"), b"x").unwrap();
    for path in [
        base.path().join("missing"),
        base.path().join("file"),
        "C:relative".into(),
        "\\\\server\\share".into(),
    ] {
        assert!(NativeTools::new(&path, [] as [&str; 0]).is_err());
    }
}
