#![cfg(not(windows))]

use raptorq::{Decoder, Encoder, EncodingPacket, ObjectTransmissionInformation, PayloadId};
use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

const MAGIC: &[u8; 8] = b"NRQFEC1\0";

const HELPER_C: &str = r#"
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <nanorq.h>

static void die(const char *message) {
  fprintf(stderr, "%s\n", message);
  exit(1);
}

static void die_errno(const char *message) {
  fprintf(stderr, "%s: %s\n", message, strerror(errno));
  exit(1);
}

static FILE *open_file(const char *path, const char *mode) {
  FILE *file = fopen(path, mode);
  if (!file) {
    die_errno(path);
  }
  return file;
}

static void read_exact(FILE *file, void *buffer, size_t len, const char *what) {
  if (fread(buffer, 1, len, file) != len) {
    fprintf(stderr, "failed to read %s\n", what);
    exit(1);
  }
}

static void write_exact(FILE *file, const void *buffer, size_t len,
                        const char *what) {
  if (fwrite(buffer, 1, len, file) != len) {
    fprintf(stderr, "failed to write %s\n", what);
    exit(1);
  }
}

static uint32_t read_be32(FILE *file, const char *what) {
  uint8_t bytes[4];
  read_exact(file, bytes, sizeof(bytes), what);
  return ((uint32_t)bytes[0] << 24) | ((uint32_t)bytes[1] << 16) |
         ((uint32_t)bytes[2] << 8) | (uint32_t)bytes[3];
}

static uint64_t read_be64(FILE *file, const char *what) {
  uint8_t bytes[8];
  read_exact(file, bytes, sizeof(bytes), what);
  uint64_t value = 0;
  for (size_t i = 0; i < sizeof(bytes); i++) {
    value = (value << 8) | bytes[i];
  }
  return value;
}

static void write_be32(FILE *file, uint32_t value, const char *what) {
  uint8_t bytes[4] = {
      (uint8_t)(value >> 24),
      (uint8_t)(value >> 16),
      (uint8_t)(value >> 8),
      (uint8_t)value,
  };
  write_exact(file, bytes, sizeof(bytes), what);
}

static void write_be64(FILE *file, uint64_t value, const char *what) {
  uint8_t bytes[8];
  for (int i = 7; i >= 0; i--) {
    bytes[i] = (uint8_t)value;
    value >>= 8;
  }
  write_exact(file, bytes, sizeof(bytes), what);
}

static uint8_t *read_file(const char *path, size_t *len) {
  FILE *file = open_file(path, "rb");
  if (fseek(file, 0, SEEK_END) != 0) {
    die_errno("fseek");
  }
  long end = ftell(file);
  if (end < 0) {
    die_errno("ftell");
  }
  if (fseek(file, 0, SEEK_SET) != 0) {
    die_errno("fseek");
  }

  *len = (size_t)end;
  uint8_t *buffer = calloc(*len ? *len : 1, 1);
  if (!buffer) {
    die_errno("calloc");
  }
  if (*len > 0) {
    read_exact(file, buffer, *len, path);
  }
  fclose(file);
  return buffer;
}

static void read_magic(FILE *fixture) {
  uint8_t magic[8];
  read_exact(fixture, magic, sizeof(magic), "fixture magic");
  if (memcmp(magic, "NRQFEC1", 7) != 0 || magic[7] != 0) {
    die("invalid fixture magic");
  }
}

static void write_magic(FILE *fixture) {
  static const uint8_t magic[8] = {'N', 'R', 'Q', 'F', 'E', 'C', '1', 0};
  write_exact(fixture, magic, sizeof(magic), "fixture magic");
}

static unsigned long parse_ulong(const char *value, const char *what) {
  char *end = NULL;
  errno = 0;
  unsigned long parsed = strtoul(value, &end, 10);
  if (errno != 0 || end == value || *end != '\0') {
    fprintf(stderr, "invalid %s: %s\n", what, value);
    exit(1);
  }
  return parsed;
}

static void write_symbol(FILE *fixture, nanorq *rq, struct ioctx *input,
                         uint8_t sbn, uint32_t esi) {
  size_t symbol_size = nanorq_symbol_size(rq);
  uint8_t *symbol = calloc(symbol_size, 1);
  if (!symbol) {
    die_errno("calloc");
  }

  size_t written = nanorq_encode(rq, symbol, esi, sbn, input);
  if (written != symbol_size) {
    fprintf(stderr, "nanorq encoded %zu bytes, expected %zu bytes\n", written,
            symbol_size);
    exit(1);
  }

  write_be32(fixture, nanorq_tag(sbn, esi), "packet tag");
  write_be32(fixture, (uint32_t)symbol_size, "packet symbol length");
  write_exact(fixture, symbol, symbol_size, "packet symbol");
  free(symbol);
}

static int decode_rust_fixture(const char *fixture_path,
                               const char *expected_path) {
  size_t expected_len = 0;
  uint8_t *expected = read_file(expected_path, &expected_len);
  uint8_t *decoded = calloc(expected_len ? expected_len : 1, 1);
  if (!decoded) {
    die_errno("calloc");
  }

  FILE *fixture = open_file(fixture_path, "rb");
  read_magic(fixture);
  uint64_t common = read_be64(fixture, "common OTI");
  uint32_t scheme = read_be32(fixture, "scheme-specific OTI");
  uint32_t packet_count = read_be32(fixture, "packet count");

  nanorq *rq = nanorq_decoder_new(common, scheme);
  if (!rq) {
    die("nanorq_decoder_new failed");
  }
  struct ioctx *output = ioctx_from_mem(decoded, expected_len ? expected_len : 1);
  if (!output) {
    die("ioctx_from_mem failed");
  }

  size_t symbol_size = nanorq_symbol_size(rq);
  for (uint32_t i = 0; i < packet_count; i++) {
    uint32_t tag = read_be32(fixture, "packet tag");
    uint32_t symbol_len = read_be32(fixture, "packet symbol length");
    if (symbol_len != symbol_size) {
      fprintf(stderr, "symbol length mismatch: got %u, expected %zu\n",
              symbol_len, symbol_size);
      exit(1);
    }
    uint8_t *symbol = malloc(symbol_len ? symbol_len : 1);
    if (!symbol) {
      die_errno("malloc");
    }
    read_exact(fixture, symbol, symbol_len, "packet symbol");

    int status = nanorq_decoder_add_symbol(rq, symbol, tag, output);
    free(symbol);
    if (status == NANORQ_SYM_ERR) {
      fprintf(stderr, "nanorq rejected symbol tag %u\n", tag);
      exit(1);
    }
  }
  fclose(fixture);

  size_t blocks = nanorq_blocks(rq);
  for (size_t sbn = 0; sbn < blocks; sbn++) {
    if (!nanorq_repair_block(rq, output, (uint8_t)sbn)) {
      fprintf(stderr, "nanorq failed to repair block %zu; missing=%zu repair=%zu\n",
              sbn, nanorq_num_missing(rq, (uint8_t)sbn),
              nanorq_num_repair(rq, (uint8_t)sbn));
      exit(1);
    }
  }

  if (memcmp(decoded, expected, expected_len) != 0) {
    for (size_t i = 0; i < expected_len; i++) {
      if (decoded[i] != expected[i]) {
        fprintf(stderr, "decoded payload mismatch at byte %zu: got %u expected %u\n",
                i, decoded[i], expected[i]);
        break;
      }
    }
    exit(1);
  }

  output->destroy(output);
  nanorq_free(rq);
  free(decoded);
  free(expected);
  return 0;
}

static int encode_nanorq_fixture(const char *payload_path,
                                 const char *fixture_path,
                                 unsigned long symbol_size,
                                 unsigned long repair_symbols) {
  size_t payload_len = 0;
  uint8_t *payload = read_file(payload_path, &payload_len);
  if (payload_len == 0) {
    die("nanorq does not support zero-length transfers");
  }
  if (symbol_size == 0 || symbol_size > UINT16_MAX) {
    die("symbol size is out of range");
  }

  nanorq *rq = nanorq_encoder_new_ex(payload_len, (uint16_t)symbol_size, 0, 1, 8);
  if (!rq) {
    die("nanorq_encoder_new_ex failed");
  }
  struct ioctx *input = ioctx_from_mem(payload, payload_len);
  if (!input) {
    die("ioctx_from_mem failed");
  }

  size_t blocks = nanorq_blocks(rq);
  uint32_t packet_count = 0;
  for (size_t sbn = 0; sbn < blocks; sbn++) {
    size_t source_symbols = nanorq_block_symbols(rq, (uint8_t)sbn);
    packet_count += (uint32_t)source_symbols;
    if (source_symbols > 1) {
      packet_count -= 1;
    }
    packet_count += (uint32_t)repair_symbols;
  }

  FILE *fixture = open_file(fixture_path, "wb");
  write_magic(fixture);
  write_be64(fixture, nanorq_oti_common(rq), "common OTI");
  write_be32(fixture, nanorq_oti_scheme_specific(rq), "scheme-specific OTI");
  write_be32(fixture, packet_count, "packet count");

  for (size_t sbn = 0; sbn < blocks; sbn++) {
    uint8_t block = (uint8_t)sbn;
    if (!nanorq_generate_symbols(rq, block, input)) {
      fprintf(stderr, "nanorq failed to generate block %zu\n", sbn);
      exit(1);
    }

    size_t source_symbols = nanorq_block_symbols(rq, block);
    for (uint32_t esi = 0; esi < source_symbols; esi++) {
      if (source_symbols > 1 && esi == 1) {
        continue;
      }
      write_symbol(fixture, rq, input, block, esi);
    }
    for (uint32_t r = 0; r < repair_symbols; r++) {
      write_symbol(fixture, rq, input, block, (uint32_t)source_symbols + r);
    }
    nanorq_encoder_cleanup(rq, block);
  }

  fclose(fixture);
  input->destroy(input);
  nanorq_free(rq);
  free(payload);
  return 0;
}

int main(int argc, char **argv) {
  if (argc >= 4 && strcmp(argv[1], "decode-rust") == 0) {
    return decode_rust_fixture(argv[2], argv[3]);
  }
  if (argc >= 6 && strcmp(argv[1], "encode-nanorq") == 0) {
    unsigned long symbol_size = parse_ulong(argv[4], "symbol size");
    unsigned long repair_symbols = parse_ulong(argv[5], "repair symbols");
    return encode_nanorq_fixture(argv[2], argv[3], symbol_size, repair_symbols);
  }

  fprintf(stderr,
          "usage:\n"
          "  %s decode-rust <fixture> <expected>\n"
          "  %s encode-nanorq <payload> <fixture> <symbol-size> <repair-symbols>\n",
          argv[0], argv[0]);
  return 1;
}
"#;

#[test]
#[ignore = "requires NANORQ_DIR pointing at a nanorq checkout with deps/oblas initialized"]
fn rust_raptorq_packets_decode_with_nanorq() {
    let nanorq_dir = require_nanorq_dir();
    let helper = build_helper(&nanorq_dir);
    let scratch = scratch_dir("rust-to-nanorq");

    let payload = deterministic_payload(1187);
    let payload_path = scratch.join("payload.bin");
    let fixture_path = scratch.join("rust-packets.rqf");
    fs::write(&payload_path, &payload).expect("write payload");

    let encoder = Encoder::with_defaults(&payload, 96);
    let config = encoder.get_config();
    let mut packets = Vec::new();
    let mut skipped_source_symbols = 0;

    for packet in encoder.get_encoded_packets(2) {
        let payload_id = packet.payload_id();
        if payload_id.encoding_symbol_id() == 1 {
            skipped_source_symbols += 1;
            continue;
        }

        packets.push((
            u32::from_be_bytes(payload_id.serialize()),
            packet.data().to_vec(),
        ));
    }

    assert!(
        skipped_source_symbols > 0,
        "fixture should drop at least one source symbol"
    );

    write_fixture(
        &fixture_path,
        Fixture {
            common_oti: common_oti(config),
            scheme_oti: scheme_oti(config),
            packets,
        },
    );

    assert_command(
        Command::new(&helper)
            .arg("decode-rust")
            .arg(&fixture_path)
            .arg(&payload_path),
        "decode Rust-generated packets with nanorq",
    );
}

#[test]
#[ignore = "requires NANORQ_DIR pointing at a nanorq checkout with deps/oblas initialized"]
fn nanorq_packets_decode_with_rust_raptorq() {
    let nanorq_dir = require_nanorq_dir();
    let helper = build_helper(&nanorq_dir);
    let scratch = scratch_dir("nanorq-to-rust");

    let payload = deterministic_payload(1237);
    let payload_path = scratch.join("payload.bin");
    let fixture_path = scratch.join("nanorq-packets.rqf");
    fs::write(&payload_path, &payload).expect("write payload");

    assert_command(
        Command::new(&helper)
            .arg("encode-nanorq")
            .arg(&payload_path)
            .arg(&fixture_path)
            .arg("96")
            .arg("2"),
        "generate nanorq packets",
    );

    let fixture = read_fixture(&fixture_path);
    let mut decoder = Decoder::new(oti_from_nanorq(fixture.common_oti, fixture.scheme_oti));
    let mut decoded = None;

    for (tag, data) in fixture.packets {
        decoded = decoder.decode(EncodingPacket::new(
            PayloadId::deserialize(&tag.to_be_bytes()),
            data,
        ));
        if decoded.is_some() {
            break;
        }
    }

    assert_eq!(decoded.as_deref(), Some(payload.as_slice()));
}

#[derive(Debug)]
struct Fixture {
    common_oti: u64,
    scheme_oti: u32,
    packets: Vec<(u32, Vec<u8>)>,
}

fn require_nanorq_dir() -> PathBuf {
    let dir = env::var_os("NANORQ_DIR")
        .map(PathBuf::from)
        .expect("set NANORQ_DIR to a sleepybishop/nanorq checkout");
    for required in [
        "include/nanorq.h",
        "lib/nanorq.c",
        "deps/oblas/oblas.c",
        "deps/oblas/octtables.h",
    ] {
        assert!(
            dir.join(required).exists(),
            "NANORQ_DIR is missing required file {required}"
        );
    }
    dir
}

fn build_helper(nanorq_dir: &Path) -> PathBuf {
    let out_dir = scratch_dir("helper");
    let source = out_dir.join("nanorq_interop.c");
    fs::write(&source, HELPER_C).expect("write C helper");

    let exe = out_dir.join("nanorq_interop");
    let cc = env::var_os("CC").unwrap_or_else(|| OsString::from("cc"));
    let mut command = Command::new(cc);
    command
        .arg("-std=c99")
        .arg("-O2")
        .arg("-D_DEFAULT_SOURCE")
        .arg("-D_FILE_OFFSET_BITS=64")
        .arg("-DOCTMAT_ALIGN=16")
        .arg("-I")
        .arg(nanorq_dir)
        .arg("-I")
        .arg(nanorq_dir.join("include"))
        .arg("-I")
        .arg(nanorq_dir.join("deps/oblas"))
        .arg("-o")
        .arg(&exe)
        .arg(&source);

    for source in [
        "lib/bitmask.c",
        "lib/io.c",
        "lib/params.c",
        "lib/precode.c",
        "lib/rand.c",
        "lib/sched.c",
        "lib/spmat.c",
        "lib/tuple.c",
        "lib/wrkmat.c",
        "lib/nanorq.c",
        "deps/oblas/gf2.c",
        "deps/oblas/oblas.c",
        "deps/oblas/octmat.c",
    ] {
        command.arg(nanorq_dir.join(source));
    }

    assert_command(&mut command, "compile nanorq interop helper");
    exe
}

fn scratch_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos();
    let dir = env::temp_dir().join(format!(
        "raptorq-datagram-fec-nanorq-{name}-{}-{nanos}",
        std::process::id()
    ));
    fs::create_dir_all(&dir).expect("create scratch dir");
    dir
}

fn deterministic_payload(len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| {
            let value = index
                .wrapping_mul(31)
                .wrapping_add(index / 7)
                .wrapping_add(13);
            (value & 0xff) as u8
        })
        .collect()
}

fn common_oti(config: ObjectTransmissionInformation) -> u64 {
    (config.transfer_length() << 24) | u64::from(config.symbol_size() - 1)
}

fn scheme_oti(config: ObjectTransmissionInformation) -> u32 {
    (u32::from(config.source_blocks() - 1) << 24)
        | (u32::from(config.sub_blocks() - 1) << 8)
        | u32::from(config.symbol_alignment())
}

fn oti_from_nanorq(common: u64, scheme: u32) -> ObjectTransmissionInformation {
    ObjectTransmissionInformation::new(
        common >> 24,
        ((common & 0xffff) + 1) as u16,
        (((scheme >> 24) & 0xff) + 1) as u8,
        (((scheme >> 8) & 0xffff) + 1) as u16,
        (scheme & 0xff) as u8,
    )
}

fn write_fixture(path: &Path, fixture: Fixture) {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(MAGIC);
    push_u64(&mut bytes, fixture.common_oti);
    push_u32(&mut bytes, fixture.scheme_oti);
    push_u32(&mut bytes, fixture.packets.len() as u32);

    for (tag, symbol) in fixture.packets {
        push_u32(&mut bytes, tag);
        push_u32(&mut bytes, symbol.len() as u32);
        bytes.extend_from_slice(&symbol);
    }

    fs::write(path, bytes).expect("write fixture");
}

fn read_fixture(path: &Path) -> Fixture {
    let bytes = fs::read(path).expect("read fixture");
    let mut cursor = Cursor::new(&bytes);
    let magic = cursor.read_array::<8>();
    assert_eq!(magic, *MAGIC, "fixture magic");
    let common_oti = cursor.read_u64();
    let scheme_oti = cursor.read_u32();
    let packet_count = cursor.read_u32();
    let mut packets = Vec::with_capacity(packet_count as usize);

    for _ in 0..packet_count {
        let tag = cursor.read_u32();
        let symbol_len = cursor.read_u32() as usize;
        packets.push((tag, cursor.read_vec(symbol_len)));
    }

    assert_eq!(cursor.remaining(), 0, "fixture has trailing bytes");
    Fixture {
        common_oti,
        scheme_oti,
        packets,
    }
}

fn push_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn push_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn read_array<const N: usize>(&mut self) -> [u8; N] {
        let end = self.position + N;
        let value = self.bytes[self.position..end]
            .try_into()
            .expect("slice length checked");
        self.position = end;
        value
    }

    fn read_u32(&mut self) -> u32 {
        u32::from_be_bytes(self.read_array())
    }

    fn read_u64(&mut self) -> u64 {
        u64::from_be_bytes(self.read_array())
    }

    fn read_vec(&mut self, len: usize) -> Vec<u8> {
        let end = self.position + len;
        let value = self.bytes[self.position..end].to_vec();
        self.position = end;
        value
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }
}

fn assert_command(command: &mut Command, action: &str) {
    let output = command.output().unwrap_or_else(|error| {
        panic!("failed to {action}: {error}");
    });

    if !output.status.success() {
        panic!(
            "failed to {action}\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
