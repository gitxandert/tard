use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    time::{SystemTime, UNIX_EPOCH},
};

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(name: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = env::temp_dir().join(format!("tard-{name}-{}-{nanos}", std::process::id()));
        fs::create_dir_all(&path).unwrap();
        Self { path }
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[test]
fn archives_and_extracts_many_tiny_files() {
    let test_dir = TestDir::new("roundtrip");
    let source = test_dir.path.join("source").join("tiny");
    let archive_dir = test_dir.path.join("archive");
    let extract_dir = test_dir.path.join("extract");
    fs::create_dir_all(&source).unwrap();
    fs::create_dir_all(&archive_dir).unwrap();
    fs::create_dir_all(&extract_dir).unwrap();

    let files = write_tiny_tree(&source, 48);

    assert_success(run_tard([source.as_path(), archive_dir.as_path()]));
    let archive = archive_dir.join("tiny.tard");
    assert!(
        archive.exists(),
        "archive was not created at {}",
        archive.display()
    );

    assert_success(run_tard([
        Path::new("-x"),
        archive.as_path(),
        extract_dir.as_path(),
    ]));
    assert_tiny_tree(&extract_dir.join("tiny"), &files);
}

#[test]
fn resume_keeps_many_tiny_files_readable() {
    let test_dir = TestDir::new("resume");
    let source = test_dir.path.join("source").join("tiny");
    let archive_dir = test_dir.path.join("archive");
    let extract_dir = test_dir.path.join("extract");
    fs::create_dir_all(&source).unwrap();
    fs::create_dir_all(&archive_dir).unwrap();
    fs::create_dir_all(&extract_dir).unwrap();

    let files = write_tiny_tree(&source, 32);

    assert_success(run_tard([source.as_path(), archive_dir.as_path()]));
    assert_success(run_tard([
        Path::new("-c"),
        source.as_path(),
        archive_dir.as_path(),
    ]));

    let archive = archive_dir.join("tiny.tard");
    assert_success(run_tard([
        Path::new("-x"),
        archive.as_path(),
        extract_dir.as_path(),
    ]));
    assert_tiny_tree(&extract_dir.join("tiny"), &files);
}

fn write_tiny_tree(root: &Path, count: usize) -> Vec<(PathBuf, String)> {
    let mut files = Vec::with_capacity(count);

    for i in 0..count {
        let rel = PathBuf::from(format!("group-{}", i % 6))
            .join(format!("nested-{}", i % 3))
            .join(format!("file-{i:02}.txt"));
        let content = format!("payload {i}");
        let path = root.join(&rel);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, &content).unwrap();
        files.push((rel, content));
    }

    files
}

fn assert_tiny_tree(root: &Path, files: &[(PathBuf, String)]) {
    for (rel, content) in files {
        let path = root.join(rel);
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            *content,
            "unexpected content at {}",
            path.display()
        );
    }
}

fn run_tard<const N: usize>(args: [&Path; N]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_tard"));
    command.args(args);
    command.output().unwrap()
}

fn assert_success(output: Output) {
    assert!(
        output.status.success(),
        "command failed with {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
