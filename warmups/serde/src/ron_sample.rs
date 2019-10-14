use ron::de::from_bytes;
use ron::ser::to_string;

#[derive(Serialize, Deserialize, Debug)]
pub enum Direction {
    Up,
    Down,
    Left,
    Right,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Move {
    steps: u32,
    direction: Direction,
}

impl Move {
    pub fn new() -> Move {
        Move {
            steps: 12,
            direction: Direction::Left,
        }
    }
}

pub fn write_to_buffer(m: &Move, buf: &mut Vec<u8>) {
    let contents = to_string(m).unwrap();
    let mut bytes = contents.as_bytes();
    buf.extend_from_slice(&mut bytes);
}

pub fn read_from_buffer(buf: &Vec<u8>) -> Option<Move> {
    from_bytes(&buf).ok()
}
