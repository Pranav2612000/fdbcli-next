use hex;

pub fn readable_key(key: &[u8]) -> String {
    let mut chars: Vec<String> = vec![];

    for c in key {
        if c.is_ascii_control() {
            chars.push(format!(r"\x{}", hex::encode(vec![*c])));
            continue;
        }
        let v = if let Ok(s) = String::from_utf8(vec![*c]) {
            s
        } else {
            format!(r"\x{}", hex::encode(vec![*c]))
        };

        chars.push(v);
    }

    chars.join("")
}
