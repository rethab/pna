pub mod commands {
    #[derive(Debug, PartialEq)]
    pub enum Command {
        Get(String),
        Set(String, String),
        Ping(Option<String>),
    }
}

pub mod parser {

    const CRLF: &str = "\r\n";
    const CR: u8 = 13; // \r
    const LF: u8 = 10; // \n

    use crate::commands::Command;
    use std::io::Read;

    type Result<T> = std::result::Result<T, String>;

    #[derive(Debug, PartialEq)]
    pub enum RespType {
        SimpleString,
        Error,
        Integer,
        BulkString,
        Array,
    }

    pub fn serialize(cmd: &Command) -> String {
        match cmd {
            Command::Get(key) => serialize_vec(vec!["GET", key]),
            Command::Set(key, value) => serialize_vec(vec!["SET", key, value]),
            Command::Ping(mb_message) => {
                let mut elems = vec!["PING"];
                if let Some(message) = mb_message {
                    elems.push(message);
                }
                serialize_vec(elems)
            }
        }
    }

    fn serialize_vec(elems: Vec<&str>) -> String {
        let mut res = String::from("*");
        res.push_str(&elems.len().to_string());
        res.push_str(CRLF);
        for elem in elems {
            res.push('$');
            res.push_str(&elem.len().to_string());
            res.push_str(CRLF);
            res.push_str(elem);
            res.push_str(CRLF);
        }
        res
    }

    pub fn deserialize<R: Read + std::fmt::Debug>(reader: &mut R) -> Result<Command> {
        let mut buf = [0; 1];
        if reader.read(&mut buf).unwrap() != 1 {
            return Err("Empty buffer".to_owned());
        }
        let first_char = String::from_utf8_lossy(&buf);
        match first_char.as_ref() {
            "*" => deserialize_arr(reader),
            c => Err(format!("Expected '*' but found '{}'", c)),
        }
    }

    fn deserialize_arr<R: Read>(reader: &mut R) -> Result<Command> {
        let arr_length = parse_integer(reader)?;

        let mut elems = Vec::with_capacity(arr_length as usize);
        for _ in 0..arr_length {
            let mut buf = [0; 1];
            reader.read(&mut buf).unwrap(); // consume $
            let str_length = parse_integer(reader)?;
            let mut string = String::new();
            for _ in 0..str_length {
                reader.read(&mut buf).unwrap();
                string.push_str(&String::from_utf8_lossy(&buf));
            }
            elems.push(string);
            reader.read(&mut buf).unwrap(); // consume \r
            reader.read(&mut buf).unwrap(); // consume \n
        }

        match elems[0].as_ref() {
            "GET" => Ok(Command::Get(elems[1].to_owned())),
            "SET" => Ok(Command::Set(elems[1].to_owned(), elems[2].to_owned())),
            "PING" => Ok(Command::Ping(elems.get(1).map(|m| m.to_owned()))),
            unhandled => Err(format!("Expected COMMAND, but found '{}'", unhandled)),
        }
    }

    pub fn parse_resp_type<R: Read>(reader: &mut R) -> Result<RespType> {
        let mut buf = [0; 1];
        if reader.read(&mut buf).unwrap() != 1 {
            return Err("parse_resp_type: Empty buffer".to_owned());
        }

        let c = String::from_utf8_lossy(&buf);
        match c.as_ref() {
            "+" => Ok(RespType::SimpleString),
            "-" => Ok(RespType::Error),
            ":" => Ok(RespType::Integer),
            "$" => Ok(RespType::BulkString),
            "*" => Ok(RespType::Array),
            c => Err(format!("Expected one of [+, -, :, $, *], but got '{}'", c)),
        }
    }

    pub fn parse_simple_string<R: Read>(reader: &mut R) -> Result<String> {
        read_to_crlf(reader)
    }

    pub fn parse_integer<R: Read>(reader: &mut R) -> Result<i32> {
        let str_int = read_to_crlf(reader).unwrap();
        str_int
            .parse()
            .map_err(|_| format!("'{}' cannot be parsed as int", str_int))
    }

    pub fn parse_error<R: Read>(reader: &mut R) -> Result<String> {
        read_to_crlf(reader)
    }

    pub fn parse_bulk_string<R: Read>(reader: &mut R) -> Result<Option<String>> {
        let length = parse_integer(reader)?;
        if length == -1 {
            return Ok(None);
        }

        let size = length as usize;
        let mut buf = Vec::with_capacity(size);
        buf.resize(size, 0);
        let n_bytes = reader.read(&mut buf).unwrap();
        if n_bytes != size {
            Err(format!(
                "Expected {} bytes in bulk string but found only {}",
                length, n_bytes
            ))
        } else {
            consume_crlf(reader)?;
            Ok(Some(String::from_utf8_lossy(&buf).to_string()))
        }
    }

    pub fn parse_array<R: Read>(reader: &mut R) -> Result<Option<Vec<Option<String>>>> {
        let length = parse_integer(reader)?;
        if length == -1 {
            return Ok(None);
        }
        let size = length as usize;
        let mut elems = Vec::with_capacity(size);
        for _ in 0..size {
            match parse_resp_type(reader)? {
                RespType::BulkString => elems.push(parse_bulk_string(reader)?),
                other => {
                    return Err(format!(
                        "Only string as currently supported in arrays, but got type {:?}",
                        other
                    ));
                }
            }
        }
        Ok(Some(elems))
    }

    fn read_to_crlf<R: Read>(reader: &mut R) -> Result<String> {
        let mut string = String::new();
        let mut buf = [0; 1];
        while reader.read(&mut buf).unwrap() == 1 {
            if buf[0] == CR {
                reader.read(&mut buf).unwrap(); // consume \n
                break;
            } else {
                string.push_str(&String::from_utf8_lossy(&buf));
            }
        }
        Ok(string)
    }

    fn consume_crlf<R: Read>(reader: &mut R) -> Result<()> {
        let mut buf = [0; 1];

        reader.read(&mut buf).unwrap();
        if buf[0] != CR {
            return Err(format!("Expected CR, but found '{}'", buf[0].to_string()));
        }

        reader.read(&mut buf).unwrap();
        if buf[0] != LF {
            return Err(format!("Expected LF, but found '{}'", buf[0].to_string()));
        }

        Ok(())
    }

    pub fn encode_simple_string(string: &str) -> Vec<u8> {
        let mut vec = Vec::with_capacity(string.len() + 3);
        vec.push('+' as u8);
        vec.extend_from_slice(string.as_bytes());
        vec.push(CR);
        vec.push(LF);
        vec
    }

    pub fn encode_null_bulk_string() -> Vec<u8> {
        ['$' as u8, '-' as u8, '1' as u8, CR, LF].to_vec()
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::*;
    use crate::parser::*;

    #[test]
    fn parse_get() {
        test_roundtrip(Command::Get("mykey".to_owned()));
    }

    #[test]
    fn parse_set() {
        test_roundtrip(Command::Set("mykey".to_owned(), "myvalue".to_owned()));
    }

    #[test]
    fn parse_ping_with_message() {
        test_roundtrip(Command::Ping(Some("foo".to_owned())));
    }

    #[test]
    fn parse_ping_empty() {
        test_roundtrip(Command::Ping(None));
    }

    fn test_roundtrip(cmd: Command) {
        let serialized = serialize(&cmd);
        let mut bytes = serialized.as_bytes();
        let parsed_cmd = deserialize(&mut bytes).unwrap();
        assert_eq!(cmd, parsed_cmd);
    }

    #[test]
    fn test_parse_simple_string() {
        let input = String::from("+OK\r\n");
        let mut bytes = input.as_bytes();
        assert_eq!(RespType::SimpleString, parse_resp_type(&mut bytes).unwrap());
        assert_eq!("OK".to_owned(), parse_simple_string(&mut bytes).unwrap());
    }

    #[test]
    fn test_parse_integer_zero() {
        let input = String::from(":0\r\n");
        let mut bytes = input.as_bytes();
        assert_eq!(RespType::Integer, parse_resp_type(&mut bytes).unwrap());
        assert_eq!(0, parse_integer(&mut bytes).unwrap());
    }

    #[test]
    fn test_parse_integer_1000() {
        let input = String::from(":1000\r\n");
        let mut bytes = input.as_bytes();
        assert_eq!(RespType::Integer, parse_resp_type(&mut bytes).unwrap());
        assert_eq!(1000, parse_integer(&mut bytes).unwrap());
    }

    #[test]
    fn test_parse_error() {
        let input = String::from("-Error message\r\n");
        let mut bytes = input.as_bytes();
        assert_eq!(RespType::Error, parse_resp_type(&mut bytes).unwrap());
        assert_eq!("Error message".to_owned(), parse_error(&mut bytes).unwrap());
    }

    #[test]
    fn test_parse_bulk_string_empty() {
        let input = String::from("$0\r\n\r\n");
        let mut bytes = input.as_bytes();
        assert_eq!(RespType::BulkString, parse_resp_type(&mut bytes).unwrap());
        assert_eq!(Some("".to_owned()), parse_bulk_string(&mut bytes).unwrap());
    }

    #[test]
    fn test_parse_bulk_string_null() {
        let input = String::from("$-1\r\n");
        let mut bytes = input.as_bytes();
        assert_eq!(RespType::BulkString, parse_resp_type(&mut bytes).unwrap());
        assert_eq!(None, parse_bulk_string(&mut bytes).unwrap());
    }

    #[test]
    fn test_parse_bulk_string() {
        let input = String::from("$6\r\nfoobar\r\n");
        let mut bytes = input.as_bytes();
        assert_eq!(RespType::BulkString, parse_resp_type(&mut bytes).unwrap());
        assert_eq!(
            Some("foobar".to_owned()),
            parse_bulk_string(&mut bytes).unwrap()
        );
    }

    #[test]
    fn test_parse_array_empty() {
        let input = String::from("*0\r\n");
        let mut bytes = input.as_bytes();
        assert_eq!(RespType::Array, parse_resp_type(&mut bytes).unwrap());
        assert_eq!(0, parse_array(&mut bytes).unwrap().unwrap().len());
    }

    #[test]
    fn test_parse_array_null() {
        let input = String::from("*-1\r\n");
        let mut bytes = input.as_bytes();
        assert_eq!(RespType::Array, parse_resp_type(&mut bytes).unwrap());
        assert_eq!(None, parse_array(&mut bytes).unwrap());
    }

    #[test]
    fn test_parse_array() {
        let input = String::from("*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n");
        let mut bytes = input.as_bytes();
        assert_eq!(RespType::Array, parse_resp_type(&mut bytes).unwrap());
        assert_eq!(
            vec![Some("foo".to_owned()), None, Some("bar".to_owned())],
            parse_array(&mut bytes).unwrap().unwrap()
        );
    }

    #[test]
    fn test_encode_simple_string() {
        assert_eq!(
            "+OK\r\n".to_owned(),
            String::from_utf8_lossy(&encode_simple_string("OK"))
        );
    }

    #[test]
    fn test_encode_null_bulk_string() {
        assert_eq!(
            "$-1\r\n".to_owned(),
            String::from_utf8_lossy(&encode_null_bulk_string())
        );
    }
}
