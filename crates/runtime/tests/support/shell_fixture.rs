use std::{
    io::{Read, Write},
    net::TcpStream,
    path::Path,
    process::{Command, Stdio},
};
fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args[1].as_str() {
        "success" | "environment" => {
            if args[1] == "success" {
                println!("cwd={}", std::env::current_dir().unwrap().display());
            }
            for key in [
                "CANARY_PARENT_SECRET",
                "GITHUB_TOKEN",
                "NALARVO_DAEMON_TOKEN",
                "PATH",
                "USERPROFILE",
            ] {
                println!(
                    "{key}={}",
                    std::env::var(key).unwrap_or_else(|_| "ABSENT".into())
                );
            }
            println!("args={:?}", &args[2..]);
            println!("CANARY=%CANARY_PARENT_SECRET%");
            std::fs::write("executed.txt", b"ok").unwrap();
        }
        "failure" => std::process::exit(7),
        "flood" => {
            print!("{}", "x".repeat(200_000));
            eprint!("{}", "e".repeat(200_000));
        }
        "tree" | "tree-failure" => {
            use std::os::windows::process::CommandExt;
            assert!(
                Command::new(std::env::current_exe().unwrap())
                    .arg("entry")
                    .creation_flags(0x01000000)
                    .spawn()
                    .is_err(),
                "breakaway must be denied"
            );
            let mut child = Command::new(std::env::current_exe().unwrap())
                .args(["descendant", &args[2]])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            let mut conn = TcpStream::connect(&args[2]).unwrap();
            writeln!(conn, "parent {}", std::process::id()).unwrap();
            // Both parent/descendant perform a delayed effect only after controller release.
            let mut byte = [0];
            conn.read_exact(&mut byte).unwrap();
            if byte[0] == b'q' {
                if args[1] == "tree-failure" {
                    std::process::exit(7);
                }
                return;
            }
            std::fs::write("parent-effect.txt", b"forbidden").unwrap();
            child.wait().unwrap();
        }
        "descendant" => {
            let mut conn = TcpStream::connect(&args[2]).unwrap();
            writeln!(conn, "descendant {}", std::process::id()).unwrap();
            let mut byte = [0];
            conn.read_exact(&mut byte).unwrap();
            std::fs::write("descendant-effect.txt", b"forbidden").unwrap();
        }
        "entry" => {
            std::fs::write(Path::new("entry-effect.txt"), b"forbidden").unwrap();
        }
        _ => panic!("unknown fixture"),
    }
}
