//! The Phase 1 expression engine (contract §4).
//!
//! Supported, exactly and only:
//!   columns, numeric/string/boolean/null literals, `= != > >= < <=`,
//!   `AND OR NOT` with parentheses, `+ - * /`, `IS [NOT] NULL`,
//!   `coalesce(a, b, ...)`, `now()`.
//!
//! Evaluation is **column-at-a-time** (one loop per batch per operator), never
//! row-at-a-time, so a 65k-row batch costs a handful of passes. Nulls follow
//! SQL three-valued logic: comparisons with NULL yield NULL, `WHERE` keeps only
//! TRUE, `AND`/`OR` are Kleene.
//!
//! Anything outside the subset is rejected at parse time with a message naming
//! the offending token — validation happens before any query is issued
//! (addendum §13).

use crate::error::LdpError;
use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int32Array,
    Int64Array, Int64Builder, NullArray, StringArray, StringBuilder, TimestampMicrosecondArray,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// AST
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column(String),
    Lit(Lit),
    Neg(Box<Expr>),
    Not(Box<Expr>),
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    Cmp {
        op: CmpOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Logic {
        op: LogicOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Arith {
        op: ArithOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Coalesce(Vec<Expr>),
    Now,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Lit {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
    Null,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicOp {
    And,
    Or,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
}

impl ArithOp {
    fn text(self) -> &'static str {
        match self {
            ArithOp::Add => "+",
            ArithOp::Sub => "-",
            ArithOp::Mul => "*",
            ArithOp::Div => "/",
        }
    }
}

/// Parse an expression, returning a message suitable for a validation report.
pub fn parse(src: &str) -> Result<Expr, LdpError> {
    let tokens = lex(src)?;
    let mut p = Parser { tokens, pos: 0 };
    let e = p.parse_or()?;
    if !matches!(p.peek(), Tok::Eof) {
        return Err(p.err(format!(
            "unexpected token {} after end of expression",
            p.peek().describe()
        )));
    }
    Ok(e)
}

// ---------------------------------------------------------------------------
// Lexer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    Int(i64),
    Float(f64),
    Str(String),
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    Plus,
    Minus,
    Star,
    Slash,
    LParen,
    RParen,
    Comma,
    And,
    Or,
    Not,
    Is,
    Null,
    True,
    False,
    Eof,
}

impl Tok {
    fn describe(&self) -> String {
        match self {
            Tok::Ident(s) => format!("identifier '{s}'"),
            Tok::Int(i) => format!("number {i}"),
            Tok::Float(f) => format!("number {f}"),
            Tok::Str(s) => format!("string '{s}'"),
            Tok::Eq => "'='".into(),
            Tok::Ne => "'!='".into(),
            Tok::Gt => "'>'".into(),
            Tok::Ge => "'>='".into(),
            Tok::Lt => "'<'".into(),
            Tok::Le => "'<='".into(),
            Tok::Plus => "'+'".into(),
            Tok::Minus => "'-'".into(),
            Tok::Star => "'*'".into(),
            Tok::Slash => "'/'".into(),
            Tok::LParen => "'('".into(),
            Tok::RParen => "')'".into(),
            Tok::Comma => "','".into(),
            Tok::And => "AND".into(),
            Tok::Or => "OR".into(),
            Tok::Not => "NOT".into(),
            Tok::Is => "IS".into(),
            Tok::Null => "NULL".into(),
            Tok::True => "TRUE".into(),
            Tok::False => "FALSE".into(),
            Tok::Eof => "end of expression".into(),
        }
    }
}

fn lex(src: &str) -> Result<Vec<Tok>, LdpError> {
    let chars: Vec<char> = src.chars().collect();
    let mut i = 0usize;
    let mut out = Vec::new();
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        // identifiers / keywords
        if c.is_ascii_alphabetic() || c == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            let upper = word.to_ascii_uppercase();
            out.push(match upper.as_str() {
                "AND" => Tok::And,
                "OR" => Tok::Or,
                "NOT" => Tok::Not,
                "IS" => Tok::Is,
                "NULL" => Tok::Null,
                "TRUE" => Tok::True,
                "FALSE" => Tok::False,
                _ => Tok::Ident(word),
            });
            continue;
        }
        // numbers
        if c.is_ascii_digit() {
            let start = i;
            while i < chars.len() && chars[i].is_ascii_digit() {
                i += 1;
            }
            let mut is_float = false;
            if i < chars.len()
                && chars[i] == '.'
                && i + 1 < chars.len()
                && chars[i + 1].is_ascii_digit()
            {
                is_float = true;
                i += 1;
                while i < chars.len() && chars[i].is_ascii_digit() {
                    i += 1;
                }
            }
            let text: String = chars[start..i].iter().collect();
            if is_float {
                let v: f64 = text
                    .parse()
                    .map_err(|_| cfg_err(format!("invalid number '{text}'")))?;
                out.push(Tok::Float(v));
            } else {
                match text.parse::<i64>() {
                    Ok(v) => out.push(Tok::Int(v)),
                    Err(_) => {
                        return Err(cfg_err(format!(
                            "integer literal '{text}' does not fit in 64 bits"
                        )))
                    }
                }
            }
            continue;
        }
        // strings, with '' as an escaped quote
        if c == '\'' {
            i += 1;
            let mut s = String::new();
            loop {
                if i >= chars.len() {
                    return Err(cfg_err("unterminated string literal (missing closing ')"));
                }
                if chars[i] == '\'' {
                    if i + 1 < chars.len() && chars[i + 1] == '\'' {
                        s.push('\'');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                s.push(chars[i]);
                i += 1;
            }
            out.push(Tok::Str(s));
            continue;
        }
        // operators
        let two: String = chars[i..(i + 2).min(chars.len())].iter().collect();
        match two.as_str() {
            "!=" | "<>" => {
                out.push(Tok::Ne);
                i += 2;
                continue;
            }
            ">=" => {
                out.push(Tok::Ge);
                i += 2;
                continue;
            }
            "<=" => {
                out.push(Tok::Le);
                i += 2;
                continue;
            }
            _ => {}
        }
        match c {
            '=' => out.push(Tok::Eq),
            '>' => out.push(Tok::Gt),
            '<' => out.push(Tok::Lt),
            '+' => out.push(Tok::Plus),
            '-' => out.push(Tok::Minus),
            '*' => out.push(Tok::Star),
            '/' => out.push(Tok::Slash),
            '(' => out.push(Tok::LParen),
            ')' => out.push(Tok::RParen),
            ',' => out.push(Tok::Comma),
            other => {
                return Err(cfg_err(format!(
                    "unsupported character '{other}' in expression \
                     (Phase 1 supports = != > >= < <= AND OR NOT IS NULL, + - * /, \
                     coalesce(), now())"
                )))
            }
        }
        i += 1;
    }
    out.push(Tok::Eof);
    Ok(out)
}

fn cfg_err(msg: impl Into<String>) -> LdpError {
    LdpError::Config(msg.into())
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

struct Parser {
    tokens: Vec<Tok>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> &Tok {
        &self.tokens[self.pos]
    }

    fn bump(&mut self) -> Tok {
        let t = self.tokens[self.pos].clone();
        self.pos += 1;
        t
    }

    fn err(&self, msg: impl Into<String>) -> LdpError {
        cfg_err(format!("expression parse error: {}", msg.into()))
    }

    fn parse_or(&mut self) -> Result<Expr, LdpError> {
        let mut left = self.parse_and()?;
        while matches!(self.peek(), Tok::Or) {
            self.bump();
            let right = self.parse_and()?;
            left = Expr::Logic {
                op: LogicOp::Or,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr, LdpError> {
        let mut left = self.parse_not()?;
        while matches!(self.peek(), Tok::And) {
            self.bump();
            let right = self.parse_not()?;
            left = Expr::Logic {
                op: LogicOp::And,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Expr, LdpError> {
        if matches!(self.peek(), Tok::Not) {
            self.bump();
            let inner = self.parse_not()?;
            return Ok(Expr::Not(Box::new(inner)));
        }
        self.parse_cmp()
    }

    fn parse_cmp(&mut self) -> Result<Expr, LdpError> {
        let left = self.parse_add()?;

        // IS [NOT] NULL
        if matches!(self.peek(), Tok::Is) {
            self.bump();
            let negated = if matches!(self.peek(), Tok::Not) {
                self.bump();
                true
            } else {
                false
            };
            if !matches!(self.peek(), Tok::Null) {
                return Err(self.err("expected NULL after IS"));
            }
            self.bump();
            return Ok(Expr::IsNull {
                expr: Box::new(left),
                negated,
            });
        }

        let op = match self.peek() {
            Tok::Eq => Some(CmpOp::Eq),
            Tok::Ne => Some(CmpOp::Ne),
            Tok::Gt => Some(CmpOp::Gt),
            Tok::Ge => Some(CmpOp::Ge),
            Tok::Lt => Some(CmpOp::Lt),
            Tok::Le => Some(CmpOp::Le),
            _ => None,
        };
        if let Some(op) = op {
            self.bump();
            // Chained comparisons (a < b < c) are not SQL and not supported.
            let right = self.parse_add()?;
            if matches!(
                self.peek(),
                Tok::Eq | Tok::Ne | Tok::Gt | Tok::Ge | Tok::Lt | Tok::Le
            ) {
                return Err(self.err("chained comparisons are not supported; use AND"));
            }
            return Ok(Expr::Cmp {
                op,
                left: Box::new(left),
                right: Box::new(right),
            });
        }
        Ok(left)
    }

    fn parse_add(&mut self) -> Result<Expr, LdpError> {
        let mut left = self.parse_mul()?;
        loop {
            let op = match self.peek() {
                Tok::Plus => ArithOp::Add,
                Tok::Minus => ArithOp::Sub,
                _ => break,
            };
            self.bump();
            let right = self.parse_mul()?;
            left = Expr::Arith {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_mul(&mut self) -> Result<Expr, LdpError> {
        let mut left = self.parse_unary()?;
        loop {
            let op = match self.peek() {
                Tok::Star => ArithOp::Mul,
                Tok::Slash => ArithOp::Div,
                _ => break,
            };
            self.bump();
            let right = self.parse_unary()?;
            left = Expr::Arith {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr, LdpError> {
        if matches!(self.peek(), Tok::Minus) {
            self.bump();
            let inner = self.parse_unary()?;
            // Fold -literal so that `amount * -1` is cheap and typed.
            return Ok(match inner {
                Expr::Lit(Lit::Int(i)) => Expr::Lit(Lit::Int(-i)),
                Expr::Lit(Lit::Float(f)) => Expr::Lit(Lit::Float(-f)),
                other => Expr::Neg(Box::new(other)),
            });
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<Expr, LdpError> {
        match self.bump() {
            Tok::Int(i) => Ok(Expr::Lit(Lit::Int(i))),
            Tok::Float(f) => Ok(Expr::Lit(Lit::Float(f))),
            Tok::Str(s) => Ok(Expr::Lit(Lit::Str(s))),
            Tok::True => Ok(Expr::Lit(Lit::Bool(true))),
            Tok::False => Ok(Expr::Lit(Lit::Bool(false))),
            Tok::Null => Ok(Expr::Lit(Lit::Null)),
            Tok::LParen => {
                let e = self.parse_or()?;
                match self.peek() {
                    Tok::RParen => {
                        self.bump();
                        Ok(e)
                    }
                    _ => Err(self.err("expected ')'")),
                }
            }
            Tok::Ident(name) => {
                if matches!(self.peek(), Tok::LParen) {
                    self.bump();
                    let mut args = Vec::new();
                    if !matches!(self.peek(), Tok::RParen) {
                        loop {
                            args.push(self.parse_or()?);
                            match self.peek() {
                                Tok::Comma => {
                                    self.bump();
                                }
                                Tok::RParen => break,
                                _ => return Err(self.err("expected ',' or ')' in function call")),
                            }
                        }
                    }
                    if !matches!(self.peek(), Tok::RParen) {
                        return Err(self.err("expected ')' to close function call"));
                    }
                    self.bump();
                    return match name.to_ascii_lowercase().as_str() {
                        "coalesce" => {
                            if args.len() < 2 {
                                return Err(self.err("coalesce() needs at least 2 arguments"));
                            }
                            Ok(Expr::Coalesce(args))
                        }
                        "now" => {
                            if !args.is_empty() {
                                return Err(self.err("now() takes no arguments"));
                            }
                            Ok(Expr::Now)
                        }
                        other => Err(self.err(format!(
                            "function '{other}()' is not supported in Phase 1 \
                             (only coalesce(), now())"
                        ))),
                    };
                }
                Ok(Expr::Column(name))
            }
            other => Err(self.err(format!("unexpected {}", other.describe()))),
        }
    }
}

// ---------------------------------------------------------------------------
// Values
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum Value {
    Null(usize),
    Bool(BooleanArray),
    I32(Int32Array),
    I64(Int64Array),
    F64(Float64Array),
    Utf8(StringArray),
    Ts(TimestampMicrosecondArray),
}

impl Value {
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Null(_) => DataType::Null,
            Value::Bool(_) => DataType::Boolean,
            Value::I32(_) => DataType::Int32,
            Value::I64(_) => DataType::Int64,
            Value::F64(_) => DataType::Float64,
            Value::Utf8(_) => DataType::Utf8,
            Value::Ts(_) => DataType::Timestamp(TimeUnit::Microsecond, None),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Value::Null(n) => *n,
            Value::Bool(a) => a.len(),
            Value::I32(a) => a.len(),
            Value::I64(a) => a.len(),
            Value::F64(a) => a.len(),
            Value::Utf8(a) => a.len(),
            Value::Ts(a) => a.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn into_array(self) -> ArrayRef {
        match self {
            Value::Null(n) => Arc::new(NullArray::new(n)),
            Value::Bool(a) => Arc::new(a),
            Value::I32(a) => Arc::new(a),
            Value::I64(a) => Arc::new(a),
            Value::F64(a) => Arc::new(a),
            Value::Utf8(a) => Arc::new(a),
            Value::Ts(a) => Arc::new(a),
        }
    }

    fn is_null_at(&self, i: usize) -> bool {
        match self {
            Value::Null(_) => true,
            Value::Bool(a) => a.is_null(i),
            Value::I32(a) => a.is_null(i),
            Value::I64(a) => a.is_null(i),
            Value::F64(a) => a.is_null(i),
            Value::Utf8(a) => a.is_null(i),
            Value::Ts(a) => a.is_null(i),
        }
    }

    fn num_at(&self, i: usize) -> Option<Num> {
        if self.is_null_at(i) {
            return None;
        }
        match self {
            Value::I32(a) => Some(Num::I(a.value(i) as i64)),
            Value::I64(a) => Some(Num::I(a.value(i))),
            Value::F64(a) => Some(Num::F(a.value(i))),
            Value::Ts(a) => Some(Num::I(a.value(i))),
            _ => None,
        }
    }

    fn bool_at(&self, i: usize) -> Option<bool> {
        match self {
            Value::Bool(a) if !a.is_null(i) => Some(a.value(i)),
            _ => None,
        }
    }

    fn str_at(&self, i: usize) -> Option<&str> {
        match self {
            Value::Utf8(a) if !a.is_null(i) => Some(a.value(i)),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Num {
    I(i64),
    F(f64),
}

impl Num {
    fn as_f64(self) -> f64 {
        match self {
            Num::I(i) => i as f64,
            Num::F(f) => f,
        }
    }
}

/// Comparison compatibility, checked at plan time so a type mismatch is a
/// configuration error rather than a per-row surprise.
fn check_comparable(l: &DataType, r: &DataType) -> Result<(), LdpError> {
    let same_type_comparable = matches!(
        l,
        DataType::Utf8
            | DataType::Boolean
            | DataType::Date32
            | DataType::Timestamp(TimeUnit::Microsecond, None)
    ) || is_numeric(l);
    let ok = *l == DataType::Null
        || *r == DataType::Null
        || (is_numeric(l) && is_numeric(r))
        || (l == r && same_type_comparable);
    if ok {
        Ok(())
    } else {
        Err(cfg_err(format!("cannot compare {l} with {r}")))
    }
}

fn is_numeric(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
    )
}

/// Widen Int32 → Int64 for any arithmetic/negation result, so the static type
/// and the runtime array always agree.
fn promoted_numeric(dt: &DataType) -> Result<DataType, LdpError> {
    match dt {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Ok(DataType::Int64),
        DataType::Float32 | DataType::Float64 => Ok(DataType::Float64),
        DataType::Null => Ok(DataType::Null),
        other => Err(cfg_err(format!(
            "arithmetic requires a numeric expression, found {other}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Type inference
// ---------------------------------------------------------------------------

pub fn expr_type(expr: &Expr, schema: &Schema) -> Result<DataType, LdpError> {
    match expr {
        Expr::Column(name) => schema
            .field_with_name(name)
            .map(|f| f.data_type().clone())
            .map_err(|_| {
                let available = schema
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                cfg_err(format!(
                    "unknown column '{name}'; available columns: [{available}]"
                ))
            }),
        Expr::Lit(Lit::Int(_)) => Ok(DataType::Int64),
        Expr::Lit(Lit::Float(_)) => Ok(DataType::Float64),
        Expr::Lit(Lit::Str(_)) => Ok(DataType::Utf8),
        Expr::Lit(Lit::Bool(_)) => Ok(DataType::Boolean),
        Expr::Lit(Lit::Null) => Ok(DataType::Null),
        Expr::Neg(inner) => promoted_numeric(&expr_type(inner, schema)?),
        Expr::Not(inner) => {
            let t = expr_type(inner, schema)?;
            if t != DataType::Boolean && t != DataType::Null {
                return Err(cfg_err(format!(
                    "NOT requires a boolean expression, found {t}"
                )));
            }
            Ok(DataType::Boolean)
        }
        Expr::IsNull { expr, .. } => {
            // Recurse so an unknown column here is still caught before the run.
            expr_type(expr, schema)?;
            Ok(DataType::Boolean)
        }
        Expr::Cmp { left, right, .. } => {
            // Descend into both sides: this is what lets validation reject a
            // typo'd column name in a condition before any SQL is issued.
            let l = expr_type(left, schema)?;
            let r = expr_type(right, schema)?;
            check_comparable(&l, &r)?;
            Ok(DataType::Boolean)
        }
        Expr::Logic { op, left, right } => {
            for side in [left, right] {
                let t = expr_type(side, schema)?;
                if t != DataType::Boolean && t != DataType::Null {
                    return Err(cfg_err(format!(
                        "{} requires boolean operands, found {t}",
                        match op {
                            LogicOp::And => "AND",
                            LogicOp::Or => "OR",
                        }
                    )));
                }
            }
            Ok(DataType::Boolean)
        }
        Expr::Arith { left, right, .. } => {
            let l = promoted_numeric(&expr_type(left, schema)?)?;
            let r = promoted_numeric(&expr_type(right, schema)?)?;
            if l == DataType::Float64 || r == DataType::Float64 {
                Ok(DataType::Float64)
            } else if l == DataType::Null && r == DataType::Null {
                Ok(DataType::Null)
            } else {
                Ok(DataType::Int64)
            }
        }
        Expr::Coalesce(args) => {
            let mut out: Option<DataType> = None;
            for a in args {
                let t = expr_type(a, schema)?;
                if t == DataType::Null {
                    continue;
                }
                out = Some(match (out, t) {
                    (None, t) => t,
                    (Some(o), t) if o == t => o,
                    (Some(o), t) if is_numeric(&o) && is_numeric(&t) => {
                        if o == DataType::Float64 || t == DataType::Float64 {
                            DataType::Float64
                        } else {
                            DataType::Int64
                        }
                    }
                    (Some(o), t) => {
                        return Err(cfg_err(format!(
                            "coalesce() arguments must share a type: found {o} and {t}"
                        )))
                    }
                });
            }
            Ok(out.unwrap_or(DataType::Null))
        }
        Expr::Now => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
    }
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

pub fn eval(expr: &Expr, batch: &arrow::record_batch::RecordBatch) -> Result<Value, LdpError> {
    let rows = batch.num_rows();
    let schema = batch.schema();
    match expr {
        Expr::Column(name) => {
            let idx = schema.index_of(name).map_err(|_| {
                let available = schema
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                LdpError::Expression(format!(
                    "unknown column '{name}'; available columns: [{available}]"
                ))
            })?;
            column_value(batch.column(idx), name)
        }
        Expr::Lit(Lit::Int(v)) => Ok(Value::I64(Int64Array::from(vec![*v; rows]))),
        Expr::Lit(Lit::Float(v)) => Ok(Value::F64(Float64Array::from(vec![*v; rows]))),
        Expr::Lit(Lit::Str(s)) => Ok(Value::Utf8(StringArray::from(vec![s.as_str(); rows]))),
        Expr::Lit(Lit::Bool(b)) => Ok(Value::Bool(BooleanArray::from(vec![*b; rows]))),
        Expr::Lit(Lit::Null) => Ok(Value::Null(rows)),
        Expr::Now => {
            let micros = chrono::Utc::now().timestamp_micros();
            Ok(Value::Ts(TimestampMicrosecondArray::from(vec![
                micros;
                rows
            ])))
        }
        Expr::Neg(inner) => {
            let v = eval(inner, batch)?;
            negate(&v).map_err(expr_err)
        }
        Expr::Not(inner) => {
            let v = eval(inner, batch)?;
            sql_not(&v).map_err(expr_err)
        }
        Expr::IsNull { expr, negated } => {
            let v = eval(expr, batch)?;
            let mut b = BooleanBuilder::with_capacity(rows);
            for i in 0..rows {
                let isnull = v.is_null_at(i);
                b.append_value(if *negated { !isnull } else { isnull });
            }
            Ok(Value::Bool(b.finish()))
        }
        Expr::Cmp { op, left, right } => {
            let l = eval(left, batch)?;
            let r = eval(right, batch)?;
            compare(*op, &l, &r).map_err(expr_err)
        }
        Expr::Logic { op, left, right } => {
            let l = eval(left, batch)?;
            let r = eval(right, batch)?;
            logic(*op, &l, &r).map_err(expr_err)
        }
        Expr::Arith { op, left, right } => {
            let l = eval(left, batch)?;
            let r = eval(right, batch)?;
            arith(*op, &l, &r).map_err(expr_err)
        }
        Expr::Coalesce(args) => {
            let mut vals = Vec::with_capacity(args.len());
            for a in args {
                vals.push(eval(a, batch)?);
            }
            coalesce(&vals, rows).map_err(expr_err)
        }
    }
}

fn expr_err(msg: String) -> LdpError {
    LdpError::Expression(msg)
}

fn column_value(arr: &ArrayRef, name: &str) -> Result<Value, LdpError> {
    match arr.data_type() {
        DataType::Boolean => Ok(Value::Bool(
            arr.as_any()
                .downcast_ref::<BooleanArray>()
                .expect("boolean")
                .clone(),
        )),
        DataType::Int32 => Ok(Value::I32(
            arr.as_any()
                .downcast_ref::<Int32Array>()
                .expect("i32")
                .clone(),
        )),
        DataType::Int64 => Ok(Value::I64(
            arr.as_any()
                .downcast_ref::<Int64Array>()
                .expect("i64")
                .clone(),
        )),
        DataType::Float64 => Ok(Value::F64(
            arr.as_any()
                .downcast_ref::<Float64Array>()
                .expect("f64")
                .clone(),
        )),
        DataType::Utf8 => Ok(Value::Utf8(
            arr.as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8")
                .clone(),
        )),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Ok(Value::Ts(
            arr.as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("ts")
                .clone(),
        )),
        DataType::Null => Ok(Value::Null(arr.len())),
        other => Err(LdpError::Expression(format!(
            "column '{name}' has type {other}, which Phase 1 expressions cannot read"
        ))),
    }
}

fn negate(v: &Value) -> Result<Value, String> {
    match v {
        Value::Null(n) => Ok(Value::Null(*n)),
        Value::I32(a) => {
            let mut b = Int64Builder::with_capacity(a.len());
            for i in 0..a.len() {
                if a.is_null(i) {
                    b.append_null();
                } else {
                    match a.value(i).checked_neg() {
                        Some(x) => b.append_value(x as i64),
                        None => b.append_null(),
                    }
                }
            }
            Ok(Value::I64(b.finish()))
        }
        Value::I64(a) => {
            let mut b = Int64Builder::with_capacity(a.len());
            for i in 0..a.len() {
                if a.is_null(i) {
                    b.append_null();
                } else {
                    match a.value(i).checked_neg() {
                        Some(x) => b.append_value(x),
                        None => b.append_null(),
                    }
                }
            }
            Ok(Value::I64(b.finish()))
        }
        Value::F64(a) => {
            let mut b = Float64Builder::with_capacity(a.len());
            for i in 0..a.len() {
                if a.is_null(i) {
                    b.append_null();
                } else {
                    b.append_value(-a.value(i));
                }
            }
            Ok(Value::F64(b.finish()))
        }
        other => Err(format!(
            "unary '-' requires a numeric expression, found {}",
            other.data_type()
        )),
    }
}

fn sql_not(v: &Value) -> Result<Value, String> {
    match v {
        Value::Null(n) => Ok(Value::Bool(BooleanArray::from(vec![None; *n]))),
        Value::Bool(a) => {
            let mut b = BooleanBuilder::with_capacity(a.len());
            for i in 0..a.len() {
                if a.is_null(i) {
                    b.append_null();
                } else {
                    b.append_value(!a.value(i));
                }
            }
            Ok(Value::Bool(b.finish()))
        }
        other => Err(format!(
            "NOT requires a boolean expression, found {}",
            other.data_type()
        )),
    }
}

fn logic(op: LogicOp, l: &Value, r: &Value) -> Result<Value, String> {
    let n = l.len().max(r.len());
    let mut b = BooleanBuilder::with_capacity(n);
    for i in 0..n {
        let lv = l.bool_at(i);
        let rv = r.bool_at(i);
        // Both sides must at least be boolean-or-null.
        for (side, v) in [("left", l), ("right", r)] {
            if !side_is_boolish(v) {
                return Err(format!(
                    "{} requires boolean operands, found {} on the {side}",
                    match op {
                        LogicOp::And => "AND",
                        LogicOp::Or => "OR",
                    },
                    v.data_type()
                ));
            }
        }
        let out = match op {
            LogicOp::And => match (lv, rv) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None, // unknown AND true
            },
            LogicOp::Or => match (lv, rv) {
                (Some(true), _) | (_, Some(true)) => Some(true),
                (Some(false), Some(false)) => Some(false),
                _ => None,
            },
        };
        match out {
            Some(v) => b.append_value(v),
            None => b.append_null(),
        }
    }
    Ok(Value::Bool(b.finish()))
}

fn side_is_boolish(v: &Value) -> bool {
    matches!(v, Value::Bool(_) | Value::Null(_))
}

fn compare(op: CmpOp, l: &Value, r: &Value) -> Result<Value, String> {
    let n = l.len().max(r.len());
    let mut b = BooleanBuilder::with_capacity(n);
    for i in 0..n {
        if l.is_null_at(i) || r.is_null_at(i) {
            b.append_null();
            continue;
        }
        let ord = compare_one(l, r, i)?;
        let res = match op {
            CmpOp::Eq => ord == std::cmp::Ordering::Equal,
            CmpOp::Ne => ord != std::cmp::Ordering::Equal,
            CmpOp::Gt => ord == std::cmp::Ordering::Greater,
            CmpOp::Ge => ord != std::cmp::Ordering::Less,
            CmpOp::Lt => ord == std::cmp::Ordering::Less,
            CmpOp::Le => ord != std::cmp::Ordering::Greater,
        };
        b.append_value(res);
    }
    Ok(Value::Bool(b.finish()))
}

fn compare_one(l: &Value, r: &Value, i: usize) -> Result<std::cmp::Ordering, String> {
    // numeric vs numeric
    if let (Some(a), Some(b)) = (l.num_at(i), r.num_at(i)) {
        // Timestamps only compare with timestamps (both land in Num::I).
        let ts_l = matches!(l, Value::Ts(_));
        let ts_r = matches!(r, Value::Ts(_));
        if ts_l != ts_r {
            return Err(format!(
                "cannot compare {} with {}",
                l.data_type(),
                r.data_type()
            ));
        }
        return Ok(match (a, b) {
            (Num::I(x), Num::I(y)) => x.cmp(&y),
            (x, y) => x
                .as_f64()
                .partial_cmp(&y.as_f64())
                .ok_or_else(|| "NaN is not comparable".to_string())?,
        });
    }
    if let (Some(a), Some(b)) = (l.str_at(i), r.str_at(i)) {
        return Ok(a.cmp(b));
    }
    if let (Some(a), Some(b)) = (l.bool_at(i), r.bool_at(i)) {
        return Ok(a.cmp(&b));
    }
    Err(format!(
        "cannot compare {} with {}",
        l.data_type(),
        r.data_type()
    ))
}

fn arith(op: ArithOp, l: &Value, r: &Value) -> Result<Value, String> {
    let n = l.len().max(r.len());
    let float = matches!(l, Value::F64(_)) || matches!(r, Value::F64(_));
    let mut fi = Float64Builder::with_capacity(n);
    let mut ii = Int64Builder::with_capacity(n);
    for i in 0..n {
        let (Some(a), Some(b)) = (l.num_at(i), r.num_at(i)) else {
            if float {
                fi.append_null();
            } else {
                ii.append_null();
            }
            continue;
        };
        if matches!(l, Value::Ts(_)) || matches!(r, Value::Ts(_)) {
            return Err("arithmetic on timestamps is not supported".into());
        }
        if float {
            let (x, y) = (a.as_f64(), b.as_f64());
            let v = match op {
                ArithOp::Add => x + y,
                ArithOp::Sub => x - y,
                ArithOp::Mul => x * y,
                ArithOp::Div => {
                    if y == 0.0 {
                        return Err("division by zero".into());
                    }
                    x / y
                }
            };
            fi.append_value(v);
        } else {
            let (Num::I(x), Num::I(y)) = (a, b) else {
                unreachable!("non-float arithmetic is integral");
            };
            let v = match op {
                ArithOp::Add => x.checked_add(y),
                ArithOp::Sub => x.checked_sub(y),
                ArithOp::Mul => x.checked_mul(y),
                ArithOp::Div => {
                    if y == 0 {
                        return Err("division by zero".into());
                    }
                    x.checked_div(y)
                }
            };
            match v {
                Some(v) => ii.append_value(v),
                None => return Err(format!("integer overflow in '{}'", op.text())),
            }
        }
    }
    if float {
        Ok(Value::F64(fi.finish()))
    } else {
        Ok(Value::I64(ii.finish()))
    }
}

fn coalesce(vals: &[Value], rows: usize) -> Result<Value, String> {
    // Result type: widest non-null argument type.
    let mut target = DataType::Null;
    for v in vals {
        let dt = v.data_type();
        if dt == DataType::Null {
            continue;
        }
        target = if target == DataType::Null {
            dt
        } else if target == dt {
            target
        } else if is_numeric(&target) && is_numeric(&dt) {
            if target == DataType::Float64 || dt == DataType::Float64 {
                DataType::Float64
            } else {
                DataType::Int64
            }
        } else {
            return Err(format!(
                "coalesce() arguments must share a type: found {target} and {dt}"
            ));
        };
    }

    // For each row, the index of the first non-null argument (if any).
    let mut picks: Vec<Option<usize>> = Vec::with_capacity(rows);
    for i in 0..rows {
        picks.push((0..vals.len()).find(|&k| !vals[k].is_null_at(i)));
    }

    match target {
        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(rows);
            for (i, &pick) in picks.iter().enumerate() {
                match pick {
                    None => b.append_null(),
                    Some(k) => {
                        let n = vals[k].num_at(i).ok_or_else(|| {
                            format!("coalesce() cannot mix {} into Float64", vals[k].data_type())
                        })?;
                        b.append_value(n.as_f64());
                    }
                }
            }
            Ok(Value::F64(b.finish()))
        }
        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(rows);
            for (i, &pick) in picks.iter().enumerate() {
                match pick {
                    None => b.append_null(),
                    Some(k) => match vals[k].num_at(i) {
                        Some(Num::I(x)) => b.append_value(x),
                        Some(Num::F(f)) => b.append_value(f as i64),
                        None => {
                            return Err(format!(
                                "coalesce() cannot mix {} into Int64",
                                vals[k].data_type()
                            ))
                        }
                    },
                }
            }
            Ok(Value::I64(b.finish()))
        }
        DataType::Utf8 => {
            let mut b = StringBuilder::with_capacity(rows, rows * 8);
            for (i, &pick) in picks.iter().enumerate() {
                match pick {
                    None => b.append_null(),
                    Some(k) => {
                        let s = vals[k].str_at(i).ok_or_else(|| {
                            format!("coalesce() cannot mix {} into Utf8", vals[k].data_type())
                        })?;
                        b.append_value(s);
                    }
                }
            }
            Ok(Value::Utf8(b.finish()))
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(rows);
            for (i, &pick) in picks.iter().enumerate() {
                match pick {
                    None => b.append_null(),
                    Some(k) => {
                        let x = vals[k].bool_at(i).ok_or_else(|| {
                            format!("coalesce() cannot mix {} into Boolean", vals[k].data_type())
                        })?;
                        b.append_value(x);
                    }
                }
            }
            Ok(Value::Bool(b.finish()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(rows);
            for (i, &pick) in picks.iter().enumerate() {
                match pick {
                    None => b.append_null(),
                    Some(k) => match &vals[k] {
                        Value::Ts(a) => b.append_value(a.value(i)),
                        other => {
                            return Err(format!(
                                "coalesce() cannot mix {} into Timestamp",
                                other.data_type()
                            ))
                        }
                    },
                }
            }
            Ok(Value::Ts(b.finish()))
        }
        DataType::Null => Ok(Value::Null(rows)),
        other => Err(format!("coalesce() cannot produce {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
    use arrow::record_batch::RecordBatch;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Float64, true),
            Field::new("status", DataType::Utf8, true),
            Field::new("flag", DataType::Boolean, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(Float64Array::from(vec![
                    Some(10.0),
                    Some(0.0),
                    None,
                    Some(-5.0),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("ACTIVE"),
                    Some("CANCELLED"),
                    Some("ACTIVE"),
                    None,
                ])),
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                    None,
                    Some(true),
                ])),
            ],
        )
        .unwrap()
    }

    fn bools(e: &str) -> Vec<Option<bool>> {
        let b = batch();
        let expr = parse(e).unwrap();
        match eval(&expr, &b).unwrap() {
            Value::Bool(a) => (0..a.len())
                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                .collect(),
            other => panic!("expected boolean, got {:?}", other.data_type()),
        }
    }

    #[test]
    fn simple_comparison_and_logic() {
        // row0 true, row1 false, row2 UNKNOWN AND TRUE = UNKNOWN,
        // row3 UNKNOWN AND FALSE = FALSE (Kleene: false wins in AND)
        assert_eq!(
            bools("status = 'ACTIVE' AND amount > 0"),
            vec![Some(true), Some(false), None, Some(false)]
        );
        // row2: UNKNOWN AND FALSE = FALSE, not UNKNOWN
        assert_eq!(
            bools("flag = true AND status IS NULL"),
            vec![Some(false), Some(false), Some(false), Some(true)]
        );
    }

    #[test]
    fn is_null_and_not_null() {
        assert_eq!(
            bools("amount IS NULL"),
            vec![Some(false), Some(false), Some(true), Some(false)]
        );
        assert_eq!(
            bools("status IS NOT NULL"),
            vec![Some(true), Some(true), Some(true), Some(false)]
        );
    }

    #[test]
    fn three_valued_null_comparison_is_unknown() {
        assert_eq!(
            bools("amount = 10"),
            vec![Some(true), Some(false), None, Some(false)]
        );
        assert_eq!(
            bools("amount != 10"),
            vec![Some(false), Some(true), None, Some(true)]
        );
    }

    #[test]
    fn arithmetic_and_precedence() {
        let b = batch();
        let e = parse("amount * 2 + 1").unwrap();
        let v = eval(&e, &b).unwrap();
        let dt = expr_type(&e, &b.schema()).unwrap();
        assert_eq!(dt, DataType::Float64);
        match v {
            Value::F64(a) => {
                assert_eq!(a.value(0), 21.0);
                assert!(a.is_null(2));
                assert_eq!(a.value(3), -9.0);
            }
            other => panic!("{other:?}"),
        }
        // 2 * (3 + 4) == 14
        let e = parse("2 * (3 + 4)").unwrap();
        match eval(&e, &b).unwrap() {
            Value::I64(a) => assert_eq!(a.value(0), 14),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn integer_division_by_zero_is_a_row_error_not_a_crash() {
        let b = batch();
        let e = parse("id / (id - 1)").unwrap();
        // Row 0 divides by zero â†’ the whole batch eval fails, which is exactly
        // what the transform's row-isolation fallback catches.
        assert!(eval(&e, &b).is_err());
    }

    #[test]
    fn coalesce_and_now() {
        let b = batch();
        let e = parse("coalesce(amount, 0.0)").unwrap();
        match eval(&e, &b).unwrap() {
            Value::F64(a) => {
                assert_eq!(a.value(0), 10.0);
                assert_eq!(a.value(2), 0.0);
            }
            other => panic!("{other:?}"),
        }
        let e = parse("now()").unwrap();
        let dt = expr_type(&e, &b.schema()).unwrap();
        assert_eq!(dt, DataType::Timestamp(TimeUnit::Microsecond, None));
        assert_eq!(eval(&e, &b).unwrap().len(), 4);
    }

    #[test]
    fn type_errors_are_caught_at_plan_time() {
        let schema = batch().schema();
        let e = parse("status + 1").unwrap();
        assert!(expr_type(&e, &schema).is_err());

        let e = parse("nosuchcolumn > 1").unwrap();
        let err = expr_type(&e, &schema).unwrap_err();
        assert!(err.to_string().contains("unknown column 'nosuchcolumn'"));
        assert!(err.to_string().contains("available columns"));
    }

    #[test]
    fn rejects_phase_two_syntax_with_a_useful_message() {
        for src in [
            "region IN ('US')",
            "lower(status) = 'x'",
            "amount BETWEEN 1 AND 2",
        ] {
            let err = parse(src).unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("not supported in Phase 1") || msg.contains("unexpected"),
                "{src} â†’ {msg}"
            );
        }
    }

    #[test]
    fn string_escapes_and_unterminated_strings() {
        let e = parse("'O''Brien'").unwrap();
        assert_eq!(e, Expr::Lit(Lit::Str("O'Brien".to_string())));
        assert!(parse("'oops").is_err());
    }

    #[test]
    fn rejects_chained_comparisons() {
        assert!(parse("a < b < c").is_err());
    }
}
