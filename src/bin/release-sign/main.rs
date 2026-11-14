//! `release-sign`: signs and checks a node release archive, in place of
//! upstream's `ant-keygen`, so publishing a release needs nothing from
//! outside this repository.
//!
//! The reference is the check every node makes before it installs a release
//! by itself: `ant_node::upgrade::verify_from_file_with_key` and
//! `verify_from_file` (ML-DSA-65 over the whole archive, with the context
//! `ant_node::upgrade::SIGNING_CONTEXT`, a raw signature of
//! `SIGNATURE_SIZE` bytes and a raw public key of `PUBLIC_KEY_SIZE` bytes).
//! `verify` calls those functions; `sign` writes a signature only after the
//! first of them has accepted it.
//!
//! The secret key is read from standard input only, as the hex the release
//! environment's `RELEASE_SIGNING_KEY_HEX` secret holds (the raw secret key,
//! as `ant-keygen generate` writes it, hex-encoded). It is never taken as an
//! argument or from a file, never printed, and no message carries any of it.
//!
//! ```text
//! release-sign check-key --public release-signing-key.pub   < secret hex
//! release-sign sign --public release-signing-key.pub --input A --output A.sig   < secret hex
//! release-sign verify --key release-signing-key.pub --input A --signature A.sig
//! release-sign verify --embedded --input A --signature A.sig
//! release-sign throwaway-key --dir DIR
//! ```

use ant_node::upgrade::{
    verify_from_file, verify_from_file_with_key, PUBLIC_KEY_SIZE, SIGNATURE_SIZE, SIGNING_CONTEXT,
};
use clap::{Parser, Subcommand};
use saorsa_pqc::api::sig::{ml_dsa_65, MlDsaPublicKey, MlDsaSecretKey, MlDsaVariant};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

/// The message `check-key` signs to prove the secret key belongs to the
/// public key: never a release, and with the release context, so the check
/// is the one a node makes.
const KEY_CHECK_MESSAGE: &[u8] = b"release-sign: does this secret key belong to this public key?";

#[derive(Parser)]
#[command(
    name = "release-sign",
    about = "Sign and check a node release archive (ML-DSA-65), as nodes check it"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Check the secret key on standard input (hex): that it is an ML-DSA-65
    /// secret key and that it belongs to the public key given.
    CheckKey {
        /// The release's public key (raw, as release-signing-key.pub).
        #[arg(long)]
        public: PathBuf,
    },
    /// Sign a file with the secret key on standard input (hex), then check
    /// the signature with the public key given, as a node does, before
    /// writing it.
    Sign {
        /// The release's public key (raw, as release-signing-key.pub).
        #[arg(long)]
        public: PathBuf,
        /// The file to sign.
        #[arg(long)]
        input: PathBuf,
        /// Where to write the signature (raw).
        #[arg(long)]
        output: PathBuf,
    },
    /// Check a signature as a node does: with the public key given, or, with
    /// --embedded, with the key compiled into this build, as a node built
    /// from the same commit checks it.
    Verify {
        /// The public key (raw, as release-signing-key.pub).
        #[arg(
            long,
            required_unless_present = "embedded",
            conflicts_with = "embedded"
        )]
        key: Option<PathBuf>,
        /// Check with the key compiled into this build.
        #[arg(long)]
        embedded: bool,
        /// The signed file.
        #[arg(long)]
        input: PathBuf,
        /// The signature (raw).
        #[arg(long)]
        signature: PathBuf,
    },
    /// Make a throwaway key pair for tests: DIR/test.pub (raw) and
    /// DIR/test.secret.hex. Never a release key.
    ThrowawayKey {
        /// The folder to write them in.
        #[arg(long)]
        dir: PathBuf,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let outcome = match cli.command {
        Command::CheckKey { public } => check_key(&public),
        Command::Sign {
            public,
            input,
            output,
        } => sign(&public, &input, &output),
        Command::Verify {
            key,
            embedded,
            input,
            signature,
        } => verify(key.as_deref(), embedded, &input, &signature),
        Command::ThrowawayKey { dir } => throwaway_key(&dir),
    };
    match outcome {
        Ok(done) => {
            println!("{done}");
            ExitCode::SUCCESS
        }
        Err(problem) => {
            eprintln!("release-sign: {problem}");
            ExitCode::FAILURE
        }
    }
}

/// The public key in `path`: raw, `PUBLIC_KEY_SIZE` bytes.
fn read_public_key(path: &Path) -> Result<MlDsaPublicKey, String> {
    let bytes = std::fs::read(path)
        .map_err(|e| format!("can't read the public key {}: {e}", path.display()))?;
    if bytes.len() != PUBLIC_KEY_SIZE {
        return Err(format!(
            "{} holds {} bytes, not an ML-DSA-65 public key ({PUBLIC_KEY_SIZE})",
            path.display(),
            bytes.len()
        ));
    }
    MlDsaPublicKey::from_bytes(MlDsaVariant::MlDsa65, &bytes)
        .map_err(|_| format!("{} is not an ML-DSA-65 public key", path.display()))
}

/// The secret key from `input`, as hex. ASCII whitespace anywhere in it,
/// line breaks included, is ignored, as the release workflow's earlier path
/// (`xxd -r -p`) ignored it, so a secret stored over several lines reads the
/// same; any other character that isn't hex is refused. Every message is
/// fixed text: none carries a byte of what was read.
fn read_secret_key(input: &mut impl Read) -> Result<MlDsaSecretKey, String> {
    // Room for a key's hex and generous whitespace up front, so the buffer
    // is not regrown, which would leave freed copies of the hex behind.
    let mut text = Vec::with_capacity(16 * 1024);
    let read = input.read_to_end(&mut text);
    let mut digits: Vec<u8> = Vec::with_capacity(text.len());
    let outcome = read
        .map_err(|_| "can't read the secret key from standard input".to_string())
        .and_then(|_| {
            digits.extend(text.iter().copied().filter(|b| !b.is_ascii_whitespace()));
            if digits.is_empty() {
                return Err("no secret key on standard input".to_string());
            }
            let mut bytes = hex::decode(&digits)
                .map_err(|_| "the secret key on standard input is not hex".to_string())?;
            let key = MlDsaSecretKey::from_bytes(MlDsaVariant::MlDsa65, &bytes).map_err(|_| {
                "the secret key on standard input is not an ML-DSA-65 secret key".to_string()
            });
            bytes.fill(0);
            key
        });
    digits.fill(0);
    text.fill(0);
    outcome
}

/// That `secret` belongs to `public`: a probe signed with it, with the
/// release context, is accepted by `public`.
fn key_belongs(secret: &MlDsaSecretKey, public: &MlDsaPublicKey) -> Result<(), String> {
    let dsa = ml_dsa_65();
    let probe = dsa
        .sign_with_context(secret, KEY_CHECK_MESSAGE, SIGNING_CONTEXT)
        .map_err(|_| "the secret key can't sign".to_string())?;
    match dsa.verify_with_context(public, KEY_CHECK_MESSAGE, &probe, SIGNING_CONTEXT) {
        Ok(true) => Ok(()),
        _ => Err("the secret key does not belong to the public key given".to_string()),
    }
}

fn check_key(public: &Path) -> Result<String, String> {
    let public = read_public_key(public)?;
    let secret = read_secret_key(&mut std::io::stdin().lock())?;
    key_belongs(&secret, &public)?;
    Ok("the secret key is an ML-DSA-65 key and belongs to the public key".to_string())
}

fn sign(public_path: &Path, input: &Path, output: &Path) -> Result<String, String> {
    let secret = read_secret_key(&mut std::io::stdin().lock())?;
    sign_with(&secret, public_path, input, output)
}

/// Sign `input` with `secret` into `output`, which is written only once the
/// node's own check, with the public key in `public_path`, has accepted it.
fn sign_with(
    secret: &MlDsaSecretKey,
    public_path: &Path,
    input: &Path,
    output: &Path,
) -> Result<String, String> {
    let public = read_public_key(public_path)?;
    key_belongs(secret, &public)?;
    let content =
        std::fs::read(input).map_err(|e| format!("can't read {}: {e}", input.display()))?;
    let signature = ml_dsa_65()
        .sign_with_context(secret, &content, SIGNING_CONTEXT)
        .map_err(|_| format!("signing {} failed", input.display()))?
        .to_bytes();
    if signature.len() != SIGNATURE_SIZE {
        return Err(format!(
            "the signature is {} bytes, not {SIGNATURE_SIZE}",
            signature.len()
        ));
    }
    // Written beside the output and moved into place only after the node's
    // check accepts it, so a signature nodes would refuse is never left.
    let pending = output.with_extension("sig-unchecked");
    std::fs::write(&pending, &signature)
        .map_err(|e| format!("can't write {}: {e}", pending.display()))?;
    if let Err(e) = verify_from_file_with_key(input, &pending, &public) {
        let _ = std::fs::remove_file(&pending);
        return Err(format!(
            "the node's check refused the new signature of {}: {e}",
            input.display()
        ));
    }
    std::fs::rename(&pending, output).map_err(|e| {
        let _ = std::fs::remove_file(&pending);
        format!("can't write {}: {e}", output.display())
    })?;
    Ok(format!(
        "signed {} into {}; the node's check accepts it",
        input.display(),
        output.display()
    ))
}

fn verify(
    key: Option<&Path>,
    embedded: bool,
    input: &Path,
    signature: &Path,
) -> Result<String, String> {
    match (key, embedded) {
        (_, true) => {
            verify_from_file(input, signature).map_err(|e| {
                format!(
                    "{} is not signed by the key compiled into this build: {e}",
                    input.display()
                )
            })?;
            Ok(format!(
                "{} is signed by the key compiled into this build",
                input.display()
            ))
        }
        (Some(key), false) => {
            let public = read_public_key(key)?;
            verify_from_file_with_key(input, signature, &public).map_err(|e| {
                format!(
                    "{} is not signed by {}: {e}",
                    input.display(),
                    key.display()
                )
            })?;
            Ok(format!(
                "{} is signed by {}",
                input.display(),
                key.display()
            ))
        }
        (None, false) => Err("give --key or --embedded".to_string()),
    }
}

fn throwaway_key(dir: &Path) -> Result<String, String> {
    let (public, secret) = ml_dsa_65()
        .generate_keypair()
        .map_err(|_| "can't make a key pair".to_string())?;
    std::fs::create_dir_all(dir).map_err(|e| format!("can't make {}: {e}", dir.display()))?;
    let public_path = dir.join("test.pub");
    std::fs::write(&public_path, public.to_bytes())
        .map_err(|e| format!("can't write {}: {e}", public_path.display()))?;
    let secret_path = dir.join("test.secret.hex");
    let mut raw = secret.to_bytes();
    let mut text = hex::encode(&raw).into_bytes();
    raw.fill(0);
    let written = std::fs::write(&secret_path, &text);
    text.fill(0);
    written.map_err(|e| format!("can't write {}: {e}", secret_path.display()))?;
    Ok(format!(
        "throwaway test key pair in {} (test.pub, test.secret.hex): never a release key",
        dir.display()
    ))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn pair() -> (MlDsaPublicKey, MlDsaSecretKey) {
        ml_dsa_65().generate_keypair().unwrap()
    }

    fn secret_hex(secret: &MlDsaSecretKey) -> Vec<u8> {
        hex::encode(secret.to_bytes()).into_bytes()
    }

    /// A signature `release-sign` makes is accepted by the node's own check,
    /// with the public key and with nothing else; a changed archive is
    /// refused.
    #[test]
    fn a_signature_it_makes_is_the_one_nodes_accept() {
        let dir = tempfile::tempdir().unwrap();
        let (public, secret) = pair();
        let public_path = dir.path().join("release.pub");
        std::fs::write(&public_path, public.to_bytes()).unwrap();
        let archive = dir.path().join("ant-node-cli-linux-x64.tar.gz");
        std::fs::write(&archive, b"an archive of a release, standing in").unwrap();
        let sig = dir.path().join("ant-node-cli-linux-x64.tar.gz.sig");

        let secret = read_secret_key(&mut secret_hex(&secret).as_slice()).unwrap();
        sign_with(&secret, &public_path, &archive, &sig).unwrap();
        assert_eq!(std::fs::read(&sig).unwrap().len(), SIGNATURE_SIZE);
        verify_from_file_with_key(&archive, &sig, &public).unwrap();
        assert!(verify(Some(&public_path), false, &archive, &sig).is_ok());

        let (other, _) = pair();
        assert!(verify_from_file_with_key(&archive, &sig, &other).is_err());
        std::fs::write(&archive, b"an archive of a release, changed").unwrap();
        assert!(verify_from_file_with_key(&archive, &sig, &public).is_err());
        // The embedded key is the production key, which signed neither.
        assert!(verify(None, true, &archive, &sig).is_err());
    }

    /// A secret key that doesn't belong to the public key given signs
    /// nothing and leaves no signature behind.
    #[test]
    fn another_key_signs_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (public, _) = pair();
        let (_, wrong) = pair();
        let public_path = dir.path().join("release.pub");
        std::fs::write(&public_path, public.to_bytes()).unwrap();
        let archive = dir.path().join("a.tar.gz");
        std::fs::write(&archive, b"x").unwrap();
        let sig = dir.path().join("a.tar.gz.sig");
        let problem = sign_with(&wrong, &public_path, &archive, &sig).unwrap_err();
        assert!(problem.contains("does not belong"), "{problem}");
        assert!(!sig.exists());
        // Nothing left beside it either: only the key and the archive.
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 2);
    }

    /// Whatever is wrong with the secret key read, the message says what,
    /// in fixed words, and carries none of what was read.
    #[test]
    fn messages_about_the_secret_key_carry_none_of_it() {
        let (_, secret) = pair();
        let good = secret_hex(&secret);
        let mut not_hex = good.clone();
        not_hex[100] = b'Q';
        let short = good[..good.len() - 2].to_vec();
        for (input, expected) in [
            (Vec::new(), "no secret key"),
            (b"  \n".to_vec(), "no secret key"),
            (not_hex, "is not hex"),
            (short, "not an ML-DSA-65 secret key"),
        ] {
            let Err(problem) = read_secret_key(&mut input.as_slice()) else {
                panic!("a bad secret key was accepted");
            };
            assert!(problem.contains(expected), "{problem}");
            for chunk in good.chunks(8) {
                assert!(
                    !problem.contains(std::str::from_utf8(chunk).unwrap()),
                    "the message carries key material: {problem}"
                );
            }
        }
        // Surrounding whitespace, as a secret pasted with a newline has, is fine.
        let mut padded = b"\n ".to_vec();
        padded.extend_from_slice(&good);
        padded.extend_from_slice(b"\n");
        assert!(read_secret_key(&mut padded.as_slice()).is_ok());
    }

    /// The hex of `secret` with `insert` after every `every` digits.
    fn with_breaks(secret: &MlDsaSecretKey, every: usize, insert: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        for chunk in secret_hex(secret).chunks(every) {
            out.extend_from_slice(chunk);
            out.extend_from_slice(insert);
        }
        out
    }

    /// Whitespace anywhere in the hex is ignored, as `xxd -r -p` ignored it
    /// in the release workflow's earlier path: a trailing newline, line
    /// breaks inside (LF or CRLF, even between the two digits of a byte),
    /// spaces and tabs. The key read is the same key.
    #[test]
    fn whitespace_anywhere_in_the_hex_is_ignored() {
        let (public, secret) = pair();
        let mut trailing = secret_hex(&secret);
        trailing.push(b'\n');
        for input in [
            trailing,
            with_breaks(&secret, 64, b"\n"),
            with_breaks(&secret, 76, b"\r\n"),
            with_breaks(&secret, 3, b"\n"),
            with_breaks(&secret, 2, b" "),
            with_breaks(&secret, 1000, b"\t\n "),
        ] {
            let read = read_secret_key(&mut input.as_slice()).expect("read");
            assert_eq!(read.to_bytes(), secret.to_bytes());
            key_belongs(&read, &public).expect("the same key");
        }
    }

    /// Any character that is neither hex nor ASCII whitespace is refused,
    /// with the fixed message, wherever it is.
    #[test]
    fn a_stray_character_is_refused() {
        let (_, secret) = pair();
        let good = secret_hex(&secret);
        for stray in [b'Z', b':', b'-', b'x', b'.', 0u8, 0xC3] {
            for at in [0, 1, good.len() / 2, good.len()] {
                let mut input = good.clone();
                input.insert(at, stray);
                let Err(problem) = read_secret_key(&mut input.as_slice()) else {
                    panic!("a stray {stray:#04x} at {at} was accepted");
                };
                assert_eq!(problem, "the secret key on standard input is not hex");
            }
        }
        let mut prefixed = b"0x".to_vec();
        prefixed.extend_from_slice(&good);
        assert!(read_secret_key(&mut prefixed.as_slice()).is_err());
    }

    /// The release key compiled into this build is a well-formed public key
    /// of the size the tool reads.
    #[test]
    fn the_embedded_check_is_the_nodes() {
        let dir = tempfile::tempdir().unwrap();
        let archive = dir.path().join("a");
        std::fs::write(&archive, b"x").unwrap();
        let sig = dir.path().join("a.sig");
        std::fs::write(&sig, vec![0u8; SIGNATURE_SIZE]).unwrap();
        let problem = verify(None, true, &archive, &sig).unwrap_err();
        assert!(problem.contains("not signed by the key compiled into this build"));
    }
}
