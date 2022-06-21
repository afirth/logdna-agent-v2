
pub fn convert_cpu_usage_to_milli(cpu: &str) -> i32{
    if cpu.is_empty()
    {
        return 0;
    }

    let value: String = cpu.chars().filter(|c| c.is_digit(10)).collect();
    let unit: String = cpu.chars().filter(|c| c.is_alphabetic()).collect();

    if value.is_empty() {
        return 0;
    }

    let parsed_value: f64 = value.parse().unwrap_or_else(|_| 0f64);
    let mut denominator= 1000000.0;

    if parsed_value < 1.0 || unit.is_empty() {
        return (parsed_value * 1000.0).ceil() as i32;
    }

    match unit.as_str() {
        "m" => {
            denominator = 1.0;
        }
        "u" => {
            denominator = 1000.0;
        }
        "n" => {}
        &_ => { error!("Unknown CPU unit") }
    }

    let result = (parsed_value/denominator).ceil() as i32;

    result
}
pub fn convert_memory_usage_to_bytes(memory: &str) -> i64{
    if memory.is_empty()
    {
        return 0;
    }

    let value: String = memory.chars().filter(|c| c.is_digit(10)).collect();
    let mut unit: String = memory.chars().filter(|c| c.is_alphabetic()).collect();

    unit = unit.to_lowercase();

    if value.is_empty() {
        return 0;
    }

    let parsed_value: i64 = value.parse().unwrap_or_else(|_| 0i64);
    let mut multiplier: i64= 1024;

    match unit.as_str() {
        "" => {
            multiplier = 1;
        }
        "ki" => {}
        "mi" => {
            multiplier = multiplier.pow(2);
        }
        "gi" => {
            multiplier = multiplier.pow(3);
        }
        "ti" => {
            multiplier = multiplier.pow(4);
        }
        "k" => {
            multiplier = 1000;
        }
        "m" => {
            multiplier = 1000000;
        }
        "g" => {
            multiplier = 1000i64.pow(3);
        }
        &_ => {}
    }
    
    return parsed_value * multiplier;
}

pub fn skip_serializing_int64(n: &i64) -> bool {
    n.is_negative()
}

pub fn skip_serializing_int32(n: &i32) -> bool {
    n.is_negative()
}
