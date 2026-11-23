use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use crate::index::{hash_value, BloomFilter, IndexKind, RangeBound};
use crate::pax::ColumnZoneMap;
use crate::types::{
    CheckConstraint, ColumnDef, ColumnType, Error, FkAction, ForeignKeyDef, Result, Row,
    TableSchema, Value,
};
use crate::vector::{VecAggKind, VecFilter};
use crate::{Database, QueryResult};

// ============================================================================
// SQL AST (with CTEs, Subqueries, Window Functions, UNIQUE/DEFAULT, ALTER ADD COLUMN)
// ============================================================================

#[derive(Debug, Clone)]
pub enum AggFunc {
    CountStar,
    Count(Box<Expr>, bool), // (expr, distinct)
    Sum(Box<Expr>),
    Avg(Box<Expr>),
    Min(Box<Expr>),
    Max(Box<Expr>),
}

#[derive(Debug, Clone)]
pub enum WindowFunc {
    RowNumber,
    Rank,
    CountStar,
    Sum(Box<Expr>),
    Avg(Box<Expr>),
}

#[derive(Debug, Clone)]
pub struct WindowSpec {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<(Expr, bool)>,
}

#[derive(Debug, Clone)]
pub enum Expr {
    Literal(Value),
    Column {
        table: Option<String>,
        name: String,
    },
    UnaryNot(Box<Expr>),
    UnaryNeg(Box<Expr>),
    Binary {
        left: Box<Expr>,
        op: BinOp,
        right: Box<Expr>,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<SelectStmt>,
        negated: bool,
    },
    Case {
        whens: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },
    Aggregate(AggFunc),
    Window {
        func: WindowFunc,
        over: WindowSpec,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
    Like,
    Add,
    Sub,
    Mul,
    Div,
}

#[derive(Debug, Clone)]
pub enum SelectItem {
    Wildcard(Option<String>),
    Expr {
        expr: Expr,
        alias: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
}

#[derive(Debug, Clone)]
pub enum TableFactor {
    Table(String),
    Subquery(Box<SelectStmt>),
}

#[derive(Debug, Clone)]
pub struct TableRef {
    pub factor: TableFactor,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct JoinClause {
    pub kind: JoinKind,
    pub table: TableRef,
    pub on: Expr,
}

#[derive(Debug, Clone)]
pub struct CteDef {
    pub name: String,
    pub query: SelectStmt,
}

#[derive(Debug, Clone)]
pub struct SelectStmt {
    pub ctes: Vec<CteDef>,
    pub distinct: bool,
    pub items: Vec<SelectItem>,
    pub from: TableRef,
    pub joins: Vec<JoinClause>,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<(Expr, bool)>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone)]
pub enum Statement {
    CreateTable {
        if_not_exists: bool,
        name: String,
        columns: Vec<ColumnDef>,
        cluster_by: Vec<String>,
        check_constraints: Vec<CheckConstraint>,
        foreign_keys: Vec<ForeignKeyDef>,
    },
    AlterClusterBy {
        table: String,
        cluster_by: Vec<String>,
    },
    AlterAddColumn {
        table: String,
        column: ColumnDef,
        check_constraints: Vec<CheckConstraint>,
        foreign_keys: Vec<ForeignKeyDef>,
    },
    DropTable {
        if_exists: bool,
        name: String,
    },
    CreateIndex {
        if_not_exists: bool,
        name: String,
        table: String,
        columns: Vec<String>,
        desc: Vec<bool>,
        kind: IndexKind,
        unique: bool,
    },
    DropIndex {
        if_exists: bool,
        name: String,
    },
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        rows: Vec<Vec<Expr>>,
    },
    Select(SelectStmt),
    Explain(SelectStmt),
    Update {
        table: String,
        assignments: Vec<(String, Expr)>,
        where_clause: Option<Expr>,
    },
    Delete {
        table: String,
        where_clause: Option<Expr>,
    },
    Begin,
    Commit,
    Rollback,
    Savepoint {
        name: String,
    },
    ReleaseSavepoint {
        name: String,
    },
    RollbackToSavepoint {
        name: String,
    },
    Vacuum {
        table_sort: Option<(String, Vec<String>)>,
    },
    Checkpoint,
    Backup {
        path: String,
    },
    PragmaStats,
    PragmaSet {
        key: String,
        value: String,
    },
    PragmaGet(String),
}

// ============================================================================
// Lexer
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    QuotedIdent(String),
    UInt(u64),
    Float(f64),
    Str(String),
    Blob(Vec<u8>),
    LParen,
    RParen,
    Comma,
    Dot,
    Semicolon,
    Star,
    Plus,
    Minus,
    Slash,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Question,
    DollarParam(usize),
    Eof,
}

fn tokenize(input: &str) -> Result<Vec<Token>> {
    let bytes = input.as_bytes();
    let mut i = 0;
    let mut tokens = Vec::new();

    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        match b {
            b'(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            b')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            b',' => {
                tokens.push(Token::Comma);
                i += 1;
            }
            b'.' => {
                tokens.push(Token::Dot);
                i += 1;
            }
            b';' => {
                tokens.push(Token::Semicolon);
                i += 1;
            }
            b'*' => {
                tokens.push(Token::Star);
                i += 1;
            }
            b'+' => {
                tokens.push(Token::Plus);
                i += 1;
            }
            b'-' => {
                tokens.push(Token::Minus);
                i += 1;
            }
            b'/' => {
                tokens.push(Token::Slash);
                i += 1;
            }
            b'=' => {
                tokens.push(Token::Eq);
                i += 1;
            }
            b'?' => {
                tokens.push(Token::Question);
                i += 1;
            }
            b'$' if i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() => {
                i += 1;
                let start = i;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                let idx: usize = input[start..i]
                    .parse()
                    .map_err(|_| Error::Sql(format!("Invalid parameter index: ${}", &input[start..i])))?;
                if idx == 0 {
                    return Err(Error::Sql("Parameter index $0 is invalid; parameters are 1-based".into()));
                }
                tokens.push(Token::DollarParam(idx));
            }
            b'!' if i + 1 < bytes.len() && bytes[i + 1] == b'=' => {
                tokens.push(Token::Ne);
                i += 2;
            }
            b'<' => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'=' {
                    tokens.push(Token::Le);
                    i += 2;
                } else if i + 1 < bytes.len() && bytes[i + 1] == b'>' {
                    tokens.push(Token::Ne);
                    i += 2;
                } else {
                    tokens.push(Token::Lt);
                    i += 1;
                }
            }
            b'>' => {
                if i + 1 < bytes.len() && bytes[i + 1] == b'=' {
                    tokens.push(Token::Ge);
                    i += 2;
                } else {
                    tokens.push(Token::Gt);
                    i += 1;
                }
            }
            b'\'' => {
                i += 1;
                let mut s = String::new();
                let mut closed = false;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                            s.push('\'');
                            i += 2;
                        } else {
                            i += 1;
                            closed = true;
                            break;
                        }
                    } else {
                        let ch = input[i..].chars().next().unwrap();
                        s.push(ch);
                        i += ch.len_utf8();
                    }
                }
                if !closed {
                    return Err(Error::Sql("Unterminated string literal".into()));
                }
                tokens.push(Token::Str(s));
            }
            b'"' | b'`' => {
                let quote = b;
                i += 1;
                let mut s = String::new();
                let mut closed = false;
                while i < bytes.len() {
                    if bytes[i] == quote {
                        if i + 1 < bytes.len() && bytes[i + 1] == quote {
                            s.push(quote as char);
                            i += 2;
                        } else {
                            i += 1;
                            closed = true;
                            break;
                        }
                    } else {
                        let ch = input[i..].chars().next().unwrap();
                        s.push(ch);
                        i += ch.len_utf8();
                    }
                }
                if !closed {
                    return Err(Error::Sql("Unterminated quoted identifier".into()));
                }
                tokens.push(Token::QuotedIdent(s));
            }
            b'x' | b'X' if i + 1 < bytes.len() && bytes[i + 1] == b'\'' => {
                i += 2;
                let start = i;
                while i < bytes.len() && bytes[i] != b'\'' {
                    i += 1;
                }
                if i >= bytes.len() {
                    return Err(Error::Sql("Unterminated hex blob literal".into()));
                }
                let hex = &input[start..i];
                i += 1;
                if !hex.is_ascii() {
                    return Err(Error::Sql("Invalid hex digit in blob literal".into()));
                }
                if hex.len() % 2 != 0 {
                    return Err(Error::Sql("Hex blob literal must have even length".into()));
                }
                let mut blob = Vec::with_capacity(hex.len() / 2);
                for k in (0..hex.len()).step_by(2) {
                    let byte = u8::from_str_radix(&hex[k..k + 2], 16)
                        .map_err(|_| Error::Sql("Invalid hex digit in blob literal".into()))?;
                    blob.push(byte);
                }
                tokens.push(Token::Blob(blob));
            }
            c if c.is_ascii_digit() => {
                let start = i;
                let mut is_float = false;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    i += 1;
                }
                if i < bytes.len() && bytes[i] == b'.' {
                    is_float = true;
                    i += 1;
                    while i < bytes.len() && bytes[i].is_ascii_digit() {
                        i += 1;
                    }
                }
                if i < bytes.len() && (bytes[i] == b'e' || bytes[i] == b'E') {
                    is_float = true;
                    i += 1;
                    if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
                        i += 1;
                    }
                    while i < bytes.len() && bytes[i].is_ascii_digit() {
                        i += 1;
                    }
                }
                let num_str = &input[start..i];
                if is_float {
                    let v: f64 = num_str
                        .parse()
                        .map_err(|_| Error::Sql(format!("Invalid float: {}", num_str)))?;
                    tokens.push(Token::Float(v));
                } else {
                    let v: u64 = num_str
                        .parse()
                        .map_err(|_| Error::Sql(format!("Invalid integer: {}", num_str)))?;
                    tokens.push(Token::UInt(v));
                }
            }
            c if c.is_ascii_alphabetic() || c == b'_' => {
                let start = i;
                while i < bytes.len()
                    && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_')
                {
                    i += 1;
                }
                tokens.push(Token::Ident(input[start..i].to_string()));
            }
            _ => {
                return Err(Error::Sql(format!(
                    "Unexpected character '{}' at byte {}",
                    b as char, i
                )));
            }
        }
    }
    tokens.push(Token::Eof);
    Ok(tokens)
}

// ============================================================================
// Recursive-Descent SQL Parser
// ============================================================================

const CLAUSE_KEYWORDS: &[&str] = &[
    "FROM", "WHERE", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "NATURAL", "OUTER", "ON",
    "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "SET", "VALUES", "UNION", "INTO", "OVER",
    "PARTITION", "AS", "AND", "OR", "ASC", "DESC", "CASE", "WHEN", "THEN", "ELSE", "END", "SELECT",
    "WITH",
];

struct Parser<'a> {
    tokens: Vec<Token>,
    pos: usize,
    params: &'a [Value],
    next_param: usize,
    used_params: Vec<bool>,
}

impl<'a> Parser<'a> {
    fn new(tokens: Vec<Token>, params: &'a [Value]) -> Self {
        let n = params.len();
        Parser {
            tokens,
            pos: 0,
            params,
            next_param: 0,
            used_params: vec![false; n],
        }
    }

    fn bind_next_param(&mut self) -> Result<Value> {
        let idx = self.next_param;
        if idx >= self.params.len() {
            return Err(Error::Sql(format!(
                "Missing SQL parameter at position {} (only {} provided)",
                idx + 1,
                self.params.len()
            )));
        }
        self.next_param += 1;
        self.used_params[idx] = true;
        Ok(self.params[idx].clone())
    }

    fn bind_numbered_param(&mut self, one_based: usize) -> Result<Value> {
        let idx = one_based - 1;
        if idx >= self.params.len() {
            return Err(Error::Sql(format!(
                "Missing SQL parameter ${} (only {} provided)",
                one_based,
                self.params.len()
            )));
        }
        self.used_params[idx] = true;
        Ok(self.params[idx].clone())
    }

    fn verify_all_params_used(&self) -> Result<()> {
        if let Some(unused_idx) = self.used_params.iter().position(|&u| !u) {
            return Err(Error::Sql(format!(
                "Unused SQL parameter at position {} ({} provided)",
                unused_idx + 1,
                self.params.len()
            )));
        }
        Ok(())
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn peek_n(&self, n: usize) -> &Token {
        self.tokens.get(self.pos + n).unwrap_or(&Token::Eof)
    }

    fn next(&mut self) -> Token {
        let t = self.peek().clone();
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
        t
    }

    fn is_kw(&self, kw: &str) -> bool {
        matches!(self.peek(), Token::Ident(s) if s.eq_ignore_ascii_case(kw))
    }

    fn eat_kw(&mut self, kw: &str) -> bool {
        if self.is_kw(kw) {
            self.next();
            true
        } else {
            false
        }
    }

    fn expect_kw(&mut self, kw: &str) -> Result<()> {
        if self.eat_kw(kw) {
            Ok(())
        } else {
            Err(Error::Sql(format!(
                "Expected keyword '{}', got {:?}",
                kw,
                self.peek()
            )))
        }
    }

    fn is_ident_tok(&self) -> bool {
        matches!(self.peek(), Token::Ident(_) | Token::QuotedIdent(_))
    }

    fn expect_ident(&mut self) -> Result<String> {
        match self.next() {
            Token::Ident(s) | Token::QuotedIdent(s) => Ok(s),
            other => Err(Error::Sql(format!(
                "Expected identifier, got {:?}",
                other
            ))),
        }
    }

    fn expect_tok(&mut self, expected: Token) -> Result<()> {
        let got = self.next();
        if got == expected {
            Ok(())
        } else {
            Err(Error::Sql(format!(
                "Expected {:?}, got {:?}",
                expected, got
            )))
        }
    }

    fn is_clause_kw(&self) -> bool {
        if let Token::Ident(s) = self.peek() {
            CLAUSE_KEYWORDS.iter().any(|k| s.eq_ignore_ascii_case(k))
        } else {
            false
        }
    }

    fn parse_ident_list_parens(&mut self) -> Result<Vec<String>> {
        self.expect_tok(Token::LParen)?;
        let mut out = Vec::new();
        loop {
            out.push(self.expect_ident()?);
            if matches!(self.peek(), Token::Comma) {
                self.next();
            } else {
                break;
            }
        }
        self.expect_tok(Token::RParen)?;
        Ok(out)
    }

    fn parse_fk_action(&mut self) -> Result<FkAction> {
        if self.eat_kw("CASCADE") {
            Ok(FkAction::Cascade)
        } else if self.eat_kw("RESTRICT") {
            Ok(FkAction::Restrict)
        } else if self.eat_kw("SET") {
            if self.eat_kw("NULL") {
                Ok(FkAction::SetNull)
            } else if self.eat_kw("DEFAULT") {
                Ok(FkAction::SetDefault)
            } else {
                Err(Error::Sql(format!(
                    "Expected NULL or DEFAULT after SET, got {:?}",
                    self.peek()
                )))
            }
        } else if self.eat_kw("NO") {
            self.expect_kw("ACTION")?;
            Ok(FkAction::NoAction)
        } else {
            Err(Error::Sql(format!(
                "Expected referential action (CASCADE, RESTRICT, SET NULL, SET DEFAULT, NO ACTION), got {:?}",
                self.peek()
            )))
        }
    }

    fn parse_fk_references(
        &mut self,
        name: Option<String>,
        columns: Vec<String>,
    ) -> Result<ForeignKeyDef> {
        let ref_table = self.expect_ident()?;
        let ref_columns = if matches!(self.peek(), Token::LParen) {
            self.parse_ident_list_parens()?
        } else {
            Vec::new()
        };
        let mut on_delete = FkAction::NoAction;
        let mut on_update = FkAction::NoAction;
        while self.eat_kw("ON") {
            if self.eat_kw("DELETE") {
                on_delete = self.parse_fk_action()?;
            } else if self.eat_kw("UPDATE") {
                on_update = self.parse_fk_action()?;
            } else {
                return Err(Error::Sql(format!(
                    "Expected DELETE or UPDATE after ON in FOREIGN KEY, got {:?}",
                    self.peek()
                )));
            }
        }
        Ok(ForeignKeyDef {
            name,
            columns,
            ref_table,
            ref_columns,
            on_delete,
            on_update,
        })
    }

    fn parse_column_def(
        &mut self,
        checks_out: &mut Vec<CheckConstraint>,
        fks_out: &mut Vec<ForeignKeyDef>,
    ) -> Result<ColumnDef> {
        let col_name = self.expect_ident()?;
        let type_word = self.expect_ident()?;
        if matches!(self.peek(), Token::LParen) {
            self.next();
            while !matches!(self.peek(), Token::RParen | Token::Eof) {
                self.next();
            }
            self.expect_tok(Token::RParen)?;
        }
        let col_type = match type_word.to_ascii_uppercase().as_str() {
            "INT" | "INTEGER" | "BIGINT" | "SMALLINT" | "TINYINT" | "BOOLEAN" | "BOOL" => {
                ColumnType::Integer
            }
            "REAL" | "FLOAT" | "DOUBLE" | "NUMERIC" | "DECIMAL" => ColumnType::Real,
            "TEXT" | "VARCHAR" | "CHAR" | "STRING" | "CLOB" => ColumnType::Text,
            "BLOB" | "BYTES" | "BINARY" | "VARBINARY" => ColumnType::Blob,
            other => {
                return Err(Error::Sql(format!("Unsupported column type: {}", other)));
            }
        };
        let mut primary_key = false;
        let mut not_null = false;
        let mut unique = false;
        let mut default_value = None;
        loop {
            let constraint_name = if self.eat_kw("CONSTRAINT") {
                Some(self.expect_ident()?)
            } else {
                None
            };
            if self.eat_kw("PRIMARY") {
                self.expect_kw("KEY")?;
                primary_key = true;
                let _ = self.eat_kw("AUTOINCREMENT");
            } else if self.eat_kw("NOT") {
                self.expect_kw("NULL")?;
                not_null = true;
            } else if self.eat_kw("UNIQUE") {
                unique = true;
            } else if self.eat_kw("DEFAULT") {
                let lit = if matches!(self.peek(), Token::LParen) {
                    self.next();
                    let v = self.parse_literal_value()?;
                    self.expect_tok(Token::RParen)?;
                    v
                } else {
                    self.parse_literal_value()?
                };
                default_value = Some(lit);
            } else if self.eat_kw("CHECK") {
                self.expect_tok(Token::LParen)?;
                let expr = self.parse_expr()?;
                self.expect_tok(Token::RParen)?;
                checks_out.push(CheckConstraint {
                    name: constraint_name,
                    expr_sql: format_expr(&expr)?,
                });
            } else if self.eat_kw("REFERENCES") {
                let fk = self.parse_fk_references(constraint_name, vec![col_name.clone()])?;
                fks_out.push(fk);
            } else if self.eat_kw("NULL") {
            } else if constraint_name.is_some() {
                return Err(Error::Sql(format!(
                    "Expected constraint after CONSTRAINT name, got {:?}",
                    self.peek()
                )));
            } else {
                break;
            }
        }
        Ok(ColumnDef {
            name: col_name,
            col_type,
            primary_key,
            not_null,
            unique,
            default_value,
        })
    }

    fn parse_literal_value(&mut self) -> Result<Value> {
        match self.next() {
            Token::UInt(v) => i64::try_from(v)
                .map(Value::Integer)
                .map_err(|_| Error::Sql(format!("Integer literal out of range: {}", v))),
            Token::Float(v) => Ok(Value::Real(v)),
            Token::Str(s) => Ok(Value::Text(s)),
            Token::Blob(b) => Ok(Value::Blob(b)),
            Token::Minus => match self.next() {
                Token::UInt(v) => {
                    if v == (i64::MAX as u64) + 1 {
                        Ok(Value::Integer(i64::MIN))
                    } else {
                        i64::try_from(v)
                            .map(|x| Value::Integer(-x))
                            .map_err(|_| Error::Sql(format!("Integer literal out of range: -{}", v)))
                    }
                }
                Token::Float(v) => Ok(Value::Real(-v)),
                other => Err(Error::Sql(format!("Invalid negative literal: {:?}", other))),
            },
            Token::Ident(s) if s.eq_ignore_ascii_case("NULL") => Ok(Value::Null),
            Token::Ident(s) if s.eq_ignore_ascii_case("TRUE") => Ok(Value::Integer(1)),
            Token::Ident(s) if s.eq_ignore_ascii_case("FALSE") => Ok(Value::Integer(0)),
            other => Err(Error::Sql(format!("Expected literal value, got {:?}", other))),
        }
    }

    fn parse_statements(&mut self) -> Result<Vec<Statement>> {
        let mut stmts = Vec::new();
        while !matches!(self.peek(), Token::Eof) {
            while matches!(self.peek(), Token::Semicolon) {
                self.next();
            }
            if matches!(self.peek(), Token::Eof) {
                break;
            }
            stmts.push(self.parse_statement()?);
            match self.peek() {
                Token::Semicolon => {
                    self.next();
                }
                Token::Eof => {}
                other => {
                    return Err(Error::Sql(format!(
                        "Expected ';' or end of input after statement, got {:?}",
                        other
                    )));
                }
            }
        }
        Ok(stmts)
    }

    fn parse_statement(&mut self) -> Result<Statement> {
        if self.eat_kw("CREATE") {
            let unique_idx = self.eat_kw("UNIQUE");
            if unique_idx || self.is_kw("INDEX") {
                self.expect_kw("INDEX")?;
                let mut if_not_exists = false;
                if self.eat_kw("IF") {
                    self.expect_kw("NOT")?;
                    self.expect_kw("EXISTS")?;
                    if_not_exists = true;
                }
                let name = self.expect_ident()?;
                self.expect_kw("ON")?;
                let table = self.expect_ident()?;
                let mut kind = IndexKind::BTree;
                if self.eat_kw("USING") {
                    let method = self.expect_ident()?;
                    kind = match method.to_ascii_uppercase().as_str() {
                        "BTREE" | "B_TREE" => IndexKind::BTree,
                        "HASH" => IndexKind::Hash,
                        other => {
                            return Err(Error::Sql(format!(
                                "Unsupported index method '{}' (use BTREE or HASH)",
                                other
                            )))
                        }
                    };
                }
                self.expect_tok(Token::LParen)?;
                let mut columns: Vec<String> = Vec::new();
                let mut desc = Vec::new();
                loop {
                    columns.push(self.expect_ident()?);
                    let is_desc = if self.eat_kw("DESC") {
                        true
                    } else {
                        let _ = self.eat_kw("ASC");
                        false
                    };
                    desc.push(is_desc);
                    if matches!(self.peek(), Token::Comma) {
                        self.next();
                    } else {
                        break;
                    }
                }
                self.expect_tok(Token::RParen)?;
                return Ok(Statement::CreateIndex {
                    if_not_exists,
                    name,
                    table,
                    columns,
                    desc,
                    kind,
                    unique: unique_idx,
                });
            }
            self.expect_kw("TABLE")?;
            let mut if_not_exists = false;
            if self.eat_kw("IF") {
                self.expect_kw("NOT")?;
                self.expect_kw("EXISTS")?;
                if_not_exists = true;
            }
            let name = self.expect_ident()?;
            self.expect_tok(Token::LParen)?;
            let mut columns: Vec<ColumnDef> = Vec::new();
            let mut check_constraints = Vec::new();
            let mut foreign_keys = Vec::new();
            loop {
                if self.is_kw("CONSTRAINT")
                    || self.is_kw("CHECK")
                    || self.is_kw("FOREIGN")
                    || self.is_kw("PRIMARY")
                    || self.is_kw("UNIQUE")
                {
                    let cname = if self.eat_kw("CONSTRAINT") {
                        Some(self.expect_ident()?)
                    } else {
                        None
                    };
                    if self.eat_kw("CHECK") {
                        self.expect_tok(Token::LParen)?;
                        let expr = self.parse_expr()?;
                        self.expect_tok(Token::RParen)?;
                        check_constraints.push(CheckConstraint {
                            name: cname,
                            expr_sql: format_expr(&expr)?,
                        });
                    } else if self.eat_kw("FOREIGN") {
                        self.expect_kw("KEY")?;
                        let fk_cols = self.parse_ident_list_parens()?;
                        self.expect_kw("REFERENCES")?;
                        let fk = self.parse_fk_references(cname, fk_cols)?;
                        foreign_keys.push(fk);
                    } else if self.eat_kw("PRIMARY") {
                        self.expect_kw("KEY")?;
                        let pk_cols = self.parse_ident_list_parens()?;
                        if pk_cols.len() == 1 {
                            if let Some(c) = columns
                                .iter_mut()
                                .find(|c| c.name.eq_ignore_ascii_case(&pk_cols[0]))
                            {
                                c.primary_key = true;
                            } else {
                                return Err(Error::Schema(format!(
                                    "Unknown PRIMARY KEY column '{}'",
                                    pk_cols[0]
                                )));
                            }
                        } else {
                            return Err(Error::Sql(
                                "Composite PRIMARY KEY is not supported; use UNIQUE index on multiple columns".into(),
                            ));
                        }
                    } else if self.eat_kw("UNIQUE") {
                        let u_cols = self.parse_ident_list_parens()?;
                        if u_cols.len() == 1 {
                            if let Some(c) = columns
                                .iter_mut()
                                .find(|c| c.name.eq_ignore_ascii_case(&u_cols[0]))
                            {
                                c.unique = true;
                            } else {
                                return Err(Error::Schema(format!(
                                    "Unknown UNIQUE column '{}'",
                                    u_cols[0]
                                )));
                            }
                        } else {
                            return Err(Error::Sql(
                                "Use CREATE UNIQUE INDEX for multi-column uniqueness".into(),
                            ));
                        }
                    } else {
                        return Err(Error::Sql(format!(
                            "Unsupported table constraint starting with {:?}",
                            self.peek()
                        )));
                    }
                } else {
                    columns.push(self.parse_column_def(&mut check_constraints, &mut foreign_keys)?);
                }
                if matches!(self.peek(), Token::Comma) {
                    self.next();
                } else {
                    break;
                }
            }
            self.expect_tok(Token::RParen)?;
            let cluster_by = if self.eat_kw("CLUSTER") {
                self.expect_kw("BY")?;
                self.parse_ident_list_parens()?
            } else {
                Vec::new()
            };
            return Ok(Statement::CreateTable {
                if_not_exists,
                name,
                columns,
                cluster_by,
                check_constraints,
                foreign_keys,
            });
        }

        if self.eat_kw("ALTER") {
            self.expect_kw("TABLE")?;
            let table = self.expect_ident()?;
            if self.eat_kw("ADD") {
                let _ = self.eat_kw("COLUMN");
                let mut check_constraints = Vec::new();
                let mut foreign_keys = Vec::new();
                let column = self.parse_column_def(&mut check_constraints, &mut foreign_keys)?;
                return Ok(Statement::AlterAddColumn {
                    table,
                    column,
                    check_constraints,
                    foreign_keys,
                });
            }
            self.expect_kw("CLUSTER")?;
            self.expect_kw("BY")?;
            let cluster_by = self.parse_ident_list_parens()?;
            return Ok(Statement::AlterClusterBy { table, cluster_by });
        }

        if self.eat_kw("DROP") {
            if self.eat_kw("INDEX") {
                let mut if_exists = false;
                if self.eat_kw("IF") {
                    self.expect_kw("EXISTS")?;
                    if_exists = true;
                }
                let name = self.expect_ident()?;
                return Ok(Statement::DropIndex { if_exists, name });
            }
            self.expect_kw("TABLE")?;
            let mut if_exists = false;
            if self.eat_kw("IF") {
                self.expect_kw("EXISTS")?;
                if_exists = true;
            }
            let name = self.expect_ident()?;
            return Ok(Statement::DropTable { if_exists, name });
        }

        if self.eat_kw("INSERT") {
            self.expect_kw("INTO")?;
            let table = self.expect_ident()?;
            let columns = if matches!(self.peek(), Token::LParen) {
                Some(self.parse_ident_list_parens()?)
            } else {
                None
            };
            self.expect_kw("VALUES")?;
            let mut rows = Vec::new();
            loop {
                self.expect_tok(Token::LParen)?;
                let mut vals = Vec::new();
                loop {
                    vals.push(self.parse_expr()?);
                    if matches!(self.peek(), Token::Comma) {
                        self.next();
                    } else {
                        break;
                    }
                }
                self.expect_tok(Token::RParen)?;
                rows.push(vals);
                if matches!(self.peek(), Token::Comma) {
                    self.next();
                } else {
                    break;
                }
            }
            return Ok(Statement::Insert {
                table,
                columns,
                rows,
            });
        }

        if self.eat_kw("EXPLAIN") {
            let sel = self.parse_query()?;
            return Ok(Statement::Explain(sel));
        }

        if self.is_kw("WITH") || self.is_kw("SELECT") {
            return Ok(Statement::Select(self.parse_query()?));
        }

        if self.eat_kw("UPDATE") {
            let table = self.expect_ident()?;
            self.expect_kw("SET")?;
            let mut assignments = Vec::new();
            loop {
                let col = self.expect_ident()?;
                self.expect_tok(Token::Eq)?;
                let val = self.parse_expr()?;
                assignments.push((col, val));
                if matches!(self.peek(), Token::Comma) {
                    self.next();
                } else {
                    break;
                }
            }
            let where_clause = if self.eat_kw("WHERE") {
                Some(self.parse_expr()?)
            } else {
                None
            };
            return Ok(Statement::Update {
                table,
                assignments,
                where_clause,
            });
        }

        if self.eat_kw("DELETE") {
            self.expect_kw("FROM")?;
            let table = self.expect_ident()?;
            let where_clause = if self.eat_kw("WHERE") {
                Some(self.parse_expr()?)
            } else {
                None
            };
            return Ok(Statement::Delete {
                table,
                where_clause,
            });
        }

        if self.eat_kw("BEGIN") {
            let _ = self.eat_kw("TRANSACTION");
            return Ok(Statement::Begin);
        }
        if self.eat_kw("COMMIT") {
            let _ = self.eat_kw("TRANSACTION");
            return Ok(Statement::Commit);
        }
        if self.eat_kw("ROLLBACK") {
            let _ = self.eat_kw("TRANSACTION");
            if self.eat_kw("TO") {
                let _ = self.eat_kw("SAVEPOINT");
                let name = self.expect_ident()?;
                return Ok(Statement::RollbackToSavepoint { name });
            }
            return Ok(Statement::Rollback);
        }
        if self.eat_kw("SAVEPOINT") {
            let name = self.expect_ident()?;
            return Ok(Statement::Savepoint { name });
        }
        if self.eat_kw("RELEASE") {
            let _ = self.eat_kw("SAVEPOINT");
            let name = self.expect_ident()?;
            return Ok(Statement::ReleaseSavepoint { name });
        }
        if self.eat_kw("SET") {
            let key = self.expect_ident()?;
            if matches!(self.peek(), Token::Eq) {
                self.next();
            } else {
                let _ = self.eat_kw("TO");
            }
            let value = match self.next() {
                Token::UInt(v) => v.to_string(),
                Token::Ident(s) | Token::QuotedIdent(s) | Token::Str(s) => s,
                other => {
                    return Err(Error::Sql(format!(
                        "Expected value after SET {}, got {:?}",
                        key, other
                    )))
                }
            };
            return Ok(Statement::PragmaSet { key, value });
        }
        if self.eat_kw("VACUUM") {
            let table_sort = if self.is_ident_tok() {
                let table = self.expect_ident()?;
                self.expect_kw("SORT")?;
                self.expect_kw("BY")?;
                let mut cols = Vec::new();
                loop {
                    cols.push(self.expect_ident()?);
                    if matches!(self.peek(), Token::Comma) {
                        self.next();
                    } else {
                        break;
                    }
                }
                Some((table, cols))
            } else {
                None
            };
            return Ok(Statement::Vacuum { table_sort });
        }
        if self.eat_kw("CHECKPOINT") {
            return Ok(Statement::Checkpoint);
        }
        if self.eat_kw("BACKUP") {
            self.expect_kw("TO")?;
            let path = match self.next() {
                Token::Str(s) => s,
                other => {
                    return Err(Error::Sql(format!(
                        "Expected backup path string literal, got {:?}",
                        other
                    )))
                }
            };
            return Ok(Statement::Backup { path });
        }
        if self.eat_kw("PRAGMA") {
            let key = self.expect_ident()?;
            if key.eq_ignore_ascii_case("STATS") {
                return Ok(Statement::PragmaStats);
            }
            if matches!(self.peek(), Token::Eq) {
                self.next();
                let value = match self.next() {
                    Token::UInt(v) => v.to_string(),
                    Token::Ident(s) | Token::QuotedIdent(s) | Token::Str(s) => s,
                    other => {
                        return Err(Error::Sql(format!(
                            "Expected value after PRAGMA {} =, got {:?}",
                            key, other
                        )))
                    }
                };
                return Ok(Statement::PragmaSet { key, value });
            }
            return Ok(Statement::PragmaGet(key));
        }

        Err(Error::Sql(format!(
            "Unsupported SQL statement starting with {:?}",
            self.peek()
        )))
    }

    fn parse_query(&mut self) -> Result<SelectStmt> {
        let mut ctes = Vec::new();
        if self.eat_kw("WITH") {
            loop {
                let name = self.expect_ident()?;
                self.expect_kw("AS")?;
                self.expect_tok(Token::LParen)?;
                let query = self.parse_query()?;
                self.expect_tok(Token::RParen)?;
                ctes.push(CteDef { name, query });
                if matches!(self.peek(), Token::Comma) {
                    self.next();
                } else {
                    break;
                }
            }
        }
        self.expect_kw("SELECT")?;
        let mut sel = self.parse_select_after_kw()?;
        sel.ctes = ctes;
        Ok(sel)
    }

    fn parse_table_ref(&mut self) -> Result<TableRef> {
        let factor = if matches!(self.peek(), Token::LParen) {
            self.next();
            let sub = self.parse_query()?;
            self.expect_tok(Token::RParen)?;
            TableFactor::Subquery(Box::new(sub))
        } else {
            TableFactor::Table(self.expect_ident()?)
        };
        let alias = if self.eat_kw("AS") {
            Some(self.expect_ident()?)
        } else if self.is_ident_tok() && !self.is_clause_kw() {
            Some(self.expect_ident()?)
        } else {
            None
        };
        Ok(TableRef { factor, alias })
    }

    fn parse_order_by_list(&mut self) -> Result<Vec<(Expr, bool)>> {
        let mut order_by = Vec::new();
        loop {
            let e = self.parse_expr()?;
            let asc = if self.eat_kw("DESC") {
                false
            } else {
                let _ = self.eat_kw("ASC");
                true
            };
            order_by.push((e, asc));
            if matches!(self.peek(), Token::Comma) {
                self.next();
            } else {
                break;
            }
        }
        Ok(order_by)
    }

    fn parse_select_after_kw(&mut self) -> Result<SelectStmt> {
        let distinct = self.eat_kw("DISTINCT");
        let mut items = Vec::new();
        loop {
            if matches!(self.peek(), Token::Star) {
                self.next();
                items.push(SelectItem::Wildcard(None));
            } else if self.is_ident_tok()
                && matches!(self.peek_n(1), Token::Dot)
                && matches!(self.peek_n(2), Token::Star)
            {
                let tbl = self.expect_ident()?;
                self.next();
                self.next();
                items.push(SelectItem::Wildcard(Some(tbl)));
            } else {
                let expr = self.parse_expr()?;
                let alias = if self.eat_kw("AS") {
                    Some(self.expect_ident()?)
                } else if self.is_ident_tok() && !self.is_clause_kw() {
                    Some(self.expect_ident()?)
                } else {
                    None
                };
                items.push(SelectItem::Expr { expr, alias });
            }
            if matches!(self.peek(), Token::Comma) {
                self.next();
            } else {
                break;
            }
        }

        self.expect_kw("FROM")?;
        let from = self.parse_table_ref()?;

        let mut joins = Vec::new();
        loop {
            let kind = if self.eat_kw("INNER") {
                self.expect_kw("JOIN")?;
                Some(JoinKind::Inner)
            } else if self.eat_kw("LEFT") {
                let _ = self.eat_kw("OUTER");
                self.expect_kw("JOIN")?;
                Some(JoinKind::Left)
            } else if self.eat_kw("JOIN") {
                Some(JoinKind::Inner)
            } else if self.eat_kw("RIGHT")
                || self.eat_kw("FULL")
                || self.eat_kw("CROSS")
                || self.eat_kw("NATURAL")
            {
                return Err(Error::Sql(
                    "Unsupported JOIN type (only INNER JOIN and LEFT [OUTER] JOIN are supported)"
                        .into(),
                ));
            } else {
                None
            };
            if let Some(kind) = kind {
                let table = self.parse_table_ref()?;
                self.expect_kw("ON")?;
                let on = self.parse_expr()?;
                joins.push(JoinClause { kind, table, on });
            } else {
                break;
            }
        }

        let where_clause = if self.eat_kw("WHERE") {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let mut group_by = Vec::new();
        if self.eat_kw("GROUP") {
            self.expect_kw("BY")?;
            loop {
                group_by.push(self.parse_expr()?);
                if matches!(self.peek(), Token::Comma) {
                    self.next();
                } else {
                    break;
                }
            }
        }

        let having = if self.eat_kw("HAVING") {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let order_by = if self.eat_kw("ORDER") {
            self.expect_kw("BY")?;
            self.parse_order_by_list()?
        } else {
            Vec::new()
        };

        let limit = if self.eat_kw("LIMIT") {
            match self.next() {
                Token::UInt(n) => {
                    let v = usize::try_from(n)
                        .map_err(|_| Error::Sql(format!("LIMIT out of range: {}", n)))?;
                    Some(v)
                }
                Token::Question => match self.bind_next_param()? {
                    Value::Integer(n) if n >= 0 => {
                        let v = usize::try_from(n)
                            .map_err(|_| Error::Sql(format!("LIMIT out of range: {}", n)))?;
                        Some(v)
                    }
                    other => return Err(Error::Sql(format!("Invalid LIMIT parameter: {:?}", other))),
                },
                Token::DollarParam(idx) => match self.bind_numbered_param(idx)? {
                    Value::Integer(n) if n >= 0 => {
                        let v = usize::try_from(n)
                            .map_err(|_| Error::Sql(format!("LIMIT out of range: {}", n)))?;
                        Some(v)
                    }
                    other => return Err(Error::Sql(format!("Invalid LIMIT parameter: {:?}", other))),
                },
                other => return Err(Error::Sql(format!("Invalid LIMIT: {:?}", other))),
            }
        } else {
            None
        };

        let offset = if self.eat_kw("OFFSET") {
            match self.next() {
                Token::UInt(n) => {
                    let v = usize::try_from(n)
                        .map_err(|_| Error::Sql(format!("OFFSET out of range: {}", n)))?;
                    Some(v)
                }
                Token::Question => match self.bind_next_param()? {
                    Value::Integer(n) if n >= 0 => {
                        let v = usize::try_from(n)
                            .map_err(|_| Error::Sql(format!("OFFSET out of range: {}", n)))?;
                        Some(v)
                    }
                    other => return Err(Error::Sql(format!("Invalid OFFSET parameter: {:?}", other))),
                },
                Token::DollarParam(idx) => match self.bind_numbered_param(idx)? {
                    Value::Integer(n) if n >= 0 => {
                        let v = usize::try_from(n)
                            .map_err(|_| Error::Sql(format!("OFFSET out of range: {}", n)))?;
                        Some(v)
                    }
                    other => return Err(Error::Sql(format!("Invalid OFFSET parameter: {:?}", other))),
                },
                other => return Err(Error::Sql(format!("Invalid OFFSET: {:?}", other))),
            }
        } else {
            None
        };

        Ok(SelectStmt {
            ctes: Vec::new(),
            distinct,
            items,
            from,
            joins,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
        })
    }

    fn parse_window_over(&mut self) -> Result<WindowSpec> {
        self.expect_tok(Token::LParen)?;
        let mut partition_by = Vec::new();
        if self.eat_kw("PARTITION") {
            self.expect_kw("BY")?;
            loop {
                partition_by.push(self.parse_expr()?);
                if matches!(self.peek(), Token::Comma) {
                    self.next();
                } else {
                    break;
                }
            }
        }
        let order_by = if self.eat_kw("ORDER") {
            self.expect_kw("BY")?;
            self.parse_order_by_list()?
        } else {
            Vec::new()
        };
        self.expect_tok(Token::RParen)?;
        Ok(WindowSpec {
            partition_by,
            order_by,
        })
    }

    fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr> {
        let mut left = self.parse_and()?;
        while self.eat_kw("OR") {
            let right = self.parse_and()?;
            left = Expr::Binary {
                left: Box::new(left),
                op: BinOp::Or,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr> {
        let mut left = self.parse_not()?;
        while self.eat_kw("AND") {
            let right = self.parse_not()?;
            left = Expr::Binary {
                left: Box::new(left),
                op: BinOp::And,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Expr> {
        if self.eat_kw("NOT") {
            return Ok(Expr::UnaryNot(Box::new(self.parse_not()?)));
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<Expr> {
        let left = self.parse_additive()?;
        if self.eat_kw("IS") {
            let negated = self.eat_kw("NOT");
            self.expect_kw("NULL")?;
            return Ok(Expr::IsNull {
                expr: Box::new(left),
                negated,
            });
        }
        let not_prefix = self.eat_kw("NOT");
        if self.eat_kw("BETWEEN") {
            let low = self.parse_additive()?;
            self.expect_kw("AND")?;
            let high = self.parse_additive()?;
            return Ok(Expr::Between {
                expr: Box::new(left),
                low: Box::new(low),
                high: Box::new(high),
                negated: not_prefix,
            });
        }
        if self.eat_kw("IN") {
            self.expect_tok(Token::LParen)?;
            if self.is_kw("SELECT") || self.is_kw("WITH") {
                let sub = self.parse_query()?;
                self.expect_tok(Token::RParen)?;
                return Ok(Expr::InSubquery {
                    expr: Box::new(left),
                    subquery: Box::new(sub),
                    negated: not_prefix,
                });
            }
            let mut list = Vec::new();
            loop {
                list.push(self.parse_expr()?);
                if matches!(self.peek(), Token::Comma) {
                    self.next();
                } else {
                    break;
                }
            }
            self.expect_tok(Token::RParen)?;
            return Ok(Expr::InList {
                expr: Box::new(left),
                list,
                negated: not_prefix,
            });
        }
        if self.eat_kw("LIKE") {
            let right = self.parse_additive()?;
            let e = Expr::Binary {
                left: Box::new(left),
                op: BinOp::Like,
                right: Box::new(right),
            };
            return Ok(if not_prefix {
                Expr::UnaryNot(Box::new(e))
            } else {
                e
            });
        }
        if not_prefix {
            return Err(Error::Sql("Unexpected NOT in expression".into()));
        }

        let op = match self.peek() {
            Token::Eq => Some(BinOp::Eq),
            Token::Ne => Some(BinOp::Ne),
            Token::Lt => Some(BinOp::Lt),
            Token::Le => Some(BinOp::Le),
            Token::Gt => Some(BinOp::Gt),
            Token::Ge => Some(BinOp::Ge),
            _ => None,
        };
        if let Some(op) = op {
            self.next();
            let right = self.parse_additive()?;
            return Ok(Expr::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            });
        }
        Ok(left)
    }

    fn parse_additive(&mut self) -> Result<Expr> {
        let mut left = self.parse_multiplicative()?;
        loop {
            let op = match self.peek() {
                Token::Plus => BinOp::Add,
                Token::Minus => BinOp::Sub,
                _ => break,
            };
            self.next();
            let right = self.parse_multiplicative()?;
            left = Expr::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_multiplicative(&mut self) -> Result<Expr> {
        let mut left = self.parse_primary()?;
        loop {
            let op = match self.peek() {
                Token::Star => BinOp::Mul,
                Token::Slash => BinOp::Div,
                _ => break,
            };
            self.next();
            let right = self.parse_primary()?;
            left = Expr::Binary {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_primary(&mut self) -> Result<Expr> {
        match self.next() {
            Token::UInt(v) => {
                let iv = i64::try_from(v)
                    .map_err(|_| Error::Sql(format!("Integer literal out of range: {}", v)))?;
                Ok(Expr::Literal(Value::Integer(iv)))
            }
            Token::Float(v) => Ok(Expr::Literal(Value::Real(v))),
            Token::Str(s) => Ok(Expr::Literal(Value::Text(s))),
            Token::Blob(b) => Ok(Expr::Literal(Value::Blob(b))),
            Token::Question => Ok(Expr::Literal(self.bind_next_param()?)),
            Token::DollarParam(idx) => Ok(Expr::Literal(self.bind_numbered_param(idx)?)),
            Token::Minus => {
                if let Token::UInt(v) = self.peek().clone() {
                    self.next();
                    if v == (i64::MAX as u64) + 1 {
                        return Ok(Expr::Literal(Value::Integer(i64::MIN)));
                    }
                    let iv = i64::try_from(v).map_err(|_| {
                        Error::Sql(format!("Integer literal out of range: -{}", v))
                    })?;
                    return Ok(Expr::Literal(Value::Integer(-iv)));
                }
                Ok(Expr::UnaryNeg(Box::new(self.parse_primary()?)))
            }
            Token::LParen => {
                let e = self.parse_expr()?;
                self.expect_tok(Token::RParen)?;
                Ok(e)
            }
            Token::QuotedIdent(s) => {
                if matches!(self.peek(), Token::Dot) {
                    self.next();
                    let col = self.expect_ident()?;
                    return Ok(Expr::Column {
                        table: Some(s),
                        name: col,
                    });
                }
                Ok(Expr::Column {
                    table: None,
                    name: s,
                })
            }
            Token::Ident(s) => {
                if s.eq_ignore_ascii_case("NULL") {
                    return Ok(Expr::Literal(Value::Null));
                }
                if s.eq_ignore_ascii_case("TRUE") {
                    return Ok(Expr::Literal(Value::Integer(1)));
                }
                if s.eq_ignore_ascii_case("FALSE") {
                    return Ok(Expr::Literal(Value::Integer(0)));
                }
                if s.eq_ignore_ascii_case("CASE") {
                    let mut whens = Vec::new();
                    while self.eat_kw("WHEN") {
                        let cond = self.parse_expr()?;
                        self.expect_kw("THEN")?;
                        let val = self.parse_expr()?;
                        whens.push((cond, val));
                    }
                    let else_expr = if self.eat_kw("ELSE") {
                        Some(Box::new(self.parse_expr()?))
                    } else {
                        None
                    };
                    self.expect_kw("END")?;
                    return Ok(Expr::Case { whens, else_expr });
                }
                // Window & Aggregate functions
                if matches!(self.peek(), Token::LParen) {
                    let upper = s.to_ascii_uppercase();
                    if matches!(upper.as_str(), "ROW_NUMBER" | "RANK") {
                        self.next();
                        self.expect_tok(Token::RParen)?;
                        self.expect_kw("OVER")?;
                        let over = self.parse_window_over()?;
                        let func = if upper == "ROW_NUMBER" {
                            WindowFunc::RowNumber
                        } else {
                            WindowFunc::Rank
                        };
                        return Ok(Expr::Window { func, over });
                    }
                    if matches!(upper.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                        self.next(); // (
                        let agg = match upper.as_str() {
                            "COUNT" => {
                                if matches!(self.peek(), Token::Star) {
                                    self.next();
                                    AggFunc::CountStar
                                } else {
                                    let distinct = self.eat_kw("DISTINCT");
                                    let arg = self.parse_expr()?;
                                    AggFunc::Count(Box::new(arg), distinct)
                                }
                            }
                            "SUM" => AggFunc::Sum(Box::new(self.parse_expr()?)),
                            "AVG" => AggFunc::Avg(Box::new(self.parse_expr()?)),
                            "MIN" => AggFunc::Min(Box::new(self.parse_expr()?)),
                            "MAX" => AggFunc::Max(Box::new(self.parse_expr()?)),
                            _ => unreachable!(),
                        };
                        self.expect_tok(Token::RParen)?;
                        if self.eat_kw("OVER") {
                            let over = self.parse_window_over()?;
                            let wf = match agg {
                                AggFunc::CountStar => WindowFunc::CountStar,
                                AggFunc::Sum(e) => WindowFunc::Sum(e),
                                AggFunc::Avg(e) => WindowFunc::Avg(e),
                                _ => {
                                    return Err(Error::Sql(
                                        "Unsupported window function with OVER(...)".into(),
                                    ))
                                }
                            };
                            return Ok(Expr::Window { func: wf, over });
                        }
                        return Ok(Expr::Aggregate(agg));
                    }
                }
                if matches!(self.peek(), Token::Dot) {
                    self.next();
                    let col = self.expect_ident()?;
                    return Ok(Expr::Column {
                        table: Some(s),
                        name: col,
                    });
                }
                Ok(Expr::Column {
                    table: None,
                    name: s,
                })
            }
            other => Err(Error::Sql(format!(
                "Unexpected token in expression: {:?}",
                other
            ))),
        }
    }
}

pub fn parse_sql(sql: &str) -> Result<Vec<Statement>> {
    parse_sql_with_params(sql, &[])
}

pub fn parse_sql_with_params(sql: &str, params: &[Value]) -> Result<Vec<Statement>> {
    let tokens = tokenize(sql)?;
    let mut parser = Parser::new(tokens, params);
    let stmts = parser.parse_statements()?;
    parser.verify_all_params_used()?;
    Ok(stmts)
}

/// Parses a standalone SQL expression string (used for CHECK constraints).
pub fn parse_expr_str(sql: &str) -> Result<Expr> {
    let tokens = tokenize(sql)?;
    let mut parser = Parser::new(tokens, &[]);
    let expr = parser.parse_expr()?;
    if !matches!(parser.peek(), Token::Eof) {
        return Err(Error::Sql(format!(
            "Unexpected trailing tokens in expression: {:?}",
            parser.peek()
        )));
    }
    Ok(expr)
}

/// Formats an `Expr` AST back into canonical SQL text (for persisting CHECK constraints).
/// Rejects subqueries, aggregates, and window expressions that are invalid inside CHECK.
pub fn format_expr(expr: &Expr) -> Result<String> {
    fn quote_ident(s: &str) -> String {
        format!("\"{}\"", s.replace('"', "\"\""))
    }
    fn fmt_val(v: &Value) -> Result<String> {
        match v {
            Value::Null => Ok("NULL".into()),
            Value::Integer(i) => Ok(i.to_string()),
            Value::Real(f) => {
                if !f.is_finite() {
                    return Err(Error::Sql(format!(
                        "Non-finite floating-point value ({}) is not valid in SQL expressions",
                        f
                    )));
                }
                let s = f.to_string();
                if !s.contains('.') && !s.contains('e') && !s.contains('E') {
                    Ok(format!("{}.0", s))
                } else {
                    Ok(s)
                }
            }
            Value::Text(s) => Ok(format!("'{}'", s.replace('\'', "''"))),
            Value::Blob(b) => {
                let mut out = String::from("X'");
                for byte in b {
                    out.push_str(&format!("{:02X}", byte));
                }
                out.push('\'');
                Ok(out)
            }
        }
    }
    match expr {
        Expr::Literal(v) => fmt_val(v),
        Expr::Column { table: None, name } => Ok(quote_ident(name)),
        Expr::Column {
            table: Some(t),
            name,
        } => Ok(format!("{}.{}", quote_ident(t), quote_ident(name))),
        Expr::UnaryNot(e) => Ok(format!("(NOT {})", format_expr(e)?)),
        Expr::UnaryNeg(e) => Ok(format!("(-{})", format_expr(e)?)),
        Expr::Binary { left, op, right } => {
            let op_s = match op {
                BinOp::Eq => "=",
                BinOp::Ne => "!=",
                BinOp::Lt => "<",
                BinOp::Le => "<=",
                BinOp::Gt => ">",
                BinOp::Ge => ">=",
                BinOp::And => "AND",
                BinOp::Or => "OR",
                BinOp::Like => "LIKE",
                BinOp::Add => "+",
                BinOp::Sub => "-",
                BinOp::Mul => "*",
                BinOp::Div => "/",
            };
            Ok(format!(
                "({} {} {})",
                format_expr(left)?,
                op_s,
                format_expr(right)?
            ))
        }
        Expr::IsNull { expr, negated } => {
            if *negated {
                Ok(format!("({} IS NOT NULL)", format_expr(expr)?))
            } else {
                Ok(format!("({} IS NULL)", format_expr(expr)?))
            }
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let kw = if *negated { "NOT BETWEEN" } else { "BETWEEN" };
            Ok(format!(
                "({} {} {} AND {})",
                format_expr(expr)?,
                kw,
                format_expr(low)?,
                format_expr(high)?
            ))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let kw = if *negated { "NOT IN" } else { "IN" };
            let mut items = Vec::with_capacity(list.len());
            for item in list {
                items.push(format_expr(item)?);
            }
            Ok(format!(
                "({} {} ({}))",
                format_expr(expr)?,
                kw,
                items.join(", ")
            ))
        }
        Expr::Case { whens, else_expr } => {
            let mut s = String::from("(CASE");
            for (c, v) in whens {
                s.push_str(&format!(" WHEN {} THEN {}", format_expr(c)?, format_expr(v)?));
            }
            if let Some(e) = else_expr {
                s.push_str(&format!(" ELSE {}", format_expr(e)?));
            }
            s.push_str(" END)");
            Ok(s)
        }
        Expr::InSubquery { .. } => Err(Error::Sql(
            "Subqueries are not allowed in CHECK constraints".into(),
        )),
        Expr::Aggregate(_) => Err(Error::Sql(
            "Aggregate functions are not allowed in CHECK constraints".into(),
        )),
        Expr::Window { .. } => Err(Error::Sql(
            "Window functions are not allowed in CHECK constraints".into(),
        )),
    }
}

// ============================================================================
// Column analysis, Secondary Index Extraction, Zone-Map & Bloom Pruning
// ============================================================================

/// Conjunctive predicates extracted for a single column (for composite & range B-tree planning).
#[derive(Debug, Clone)]
pub struct ColumnPredicates {
    /// Equality values (`col = v` or `col IN (v1, v2, ...)`).
    pub eq_values: Option<Vec<Value>>,
    pub range_lo: RangeBound,
    pub range_hi: RangeBound,
}

impl Default for ColumnPredicates {
    fn default() -> Self {
        ColumnPredicates {
            eq_values: None,
            range_lo: RangeBound::Unbounded,
            range_hi: RangeBound::Unbounded,
        }
    }
}

impl ColumnPredicates {
    pub fn has_range(&self) -> bool {
        !matches!(self.range_lo, RangeBound::Unbounded)
            || !matches!(self.range_hi, RangeBound::Unbounded)
    }

    fn tighten_lo(&mut self, bound: RangeBound) {
        self.range_lo = match (&self.range_lo, bound) {
            (RangeBound::Unbounded, b) => b,
            (RangeBound::Included(cur), RangeBound::Included(new)) => {
                if new.sql_cmp(cur).is_gt() {
                    RangeBound::Included(new)
                } else {
                    RangeBound::Included(cur.clone())
                }
            }
            (RangeBound::Included(cur), RangeBound::Excluded(new)) => {
                if !new.sql_cmp(cur).is_lt() {
                    RangeBound::Excluded(new)
                } else {
                    RangeBound::Included(cur.clone())
                }
            }
            (RangeBound::Excluded(cur), RangeBound::Included(new)) => {
                if new.sql_cmp(cur).is_gt() {
                    RangeBound::Included(new)
                } else {
                    RangeBound::Excluded(cur.clone())
                }
            }
            (RangeBound::Excluded(cur), RangeBound::Excluded(new)) => {
                if new.sql_cmp(cur).is_gt() {
                    RangeBound::Excluded(new)
                } else {
                    RangeBound::Excluded(cur.clone())
                }
            }
            (cur, RangeBound::Unbounded) => cur.clone(),
        };
    }

    fn tighten_hi(&mut self, bound: RangeBound) {
        self.range_hi = match (&self.range_hi, bound) {
            (RangeBound::Unbounded, b) => b,
            (RangeBound::Included(cur), RangeBound::Included(new)) => {
                if new.sql_cmp(cur).is_lt() {
                    RangeBound::Included(new)
                } else {
                    RangeBound::Included(cur.clone())
                }
            }
            (RangeBound::Included(cur), RangeBound::Excluded(new)) => {
                if !new.sql_cmp(cur).is_gt() {
                    RangeBound::Excluded(new)
                } else {
                    RangeBound::Included(cur.clone())
                }
            }
            (RangeBound::Excluded(cur), RangeBound::Included(new)) => {
                if new.sql_cmp(cur).is_lt() {
                    RangeBound::Included(new)
                } else {
                    RangeBound::Excluded(cur.clone())
                }
            }
            (RangeBound::Excluded(cur), RangeBound::Excluded(new)) => {
                if new.sql_cmp(cur).is_lt() {
                    RangeBound::Excluded(new)
                } else {
                    RangeBound::Excluded(cur.clone())
                }
            }
            (cur, RangeBound::Unbounded) => cur.clone(),
        };
    }
}

/// Extracts conjunctive equality, IN-list, and range predicates per column (lowercase key).
pub fn extract_column_predicates(expr: &Expr) -> HashMap<String, ColumnPredicates> {
    let mut map: HashMap<String, ColumnPredicates> = HashMap::new();
    collect_col_preds(expr, &mut map);
    map
}

fn collect_col_preds(expr: &Expr, map: &mut HashMap<String, ColumnPredicates>) {
    match expr {
        Expr::Binary {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_col_preds(left, map);
            collect_col_preds(right, map);
        }
        Expr::Binary { left, op, right } => {
            let (col_name, lit, cmp_op) = match (left.as_ref(), right.as_ref()) {
                (Expr::Column { name, .. }, Expr::Literal(v)) if !v.is_null() => (name, v, *op),
                (Expr::Literal(v), Expr::Column { name, .. }) if !v.is_null() => {
                    let flipped = match op {
                        BinOp::Eq => BinOp::Eq,
                        BinOp::Lt => BinOp::Gt,
                        BinOp::Le => BinOp::Ge,
                        BinOp::Gt => BinOp::Lt,
                        BinOp::Ge => BinOp::Le,
                        _ => return,
                    };
                    (name, v, flipped)
                }
                _ => return,
            };
            let entry = map.entry(col_name.to_ascii_lowercase()).or_default();
            match cmp_op {
                BinOp::Eq => {
                    match &mut entry.eq_values {
                        None => entry.eq_values = Some(vec![lit.clone()]),
                        Some(existing) => {
                            existing.retain(|v| v.sql_cmp(lit).is_eq());
                        }
                    }
                }
                BinOp::Gt => entry.tighten_lo(RangeBound::Excluded(lit.clone())),
                BinOp::Ge => entry.tighten_lo(RangeBound::Included(lit.clone())),
                BinOp::Lt => entry.tighten_hi(RangeBound::Excluded(lit.clone())),
                BinOp::Le => entry.tighten_hi(RangeBound::Included(lit.clone())),
                _ => {}
            }
        }
        Expr::Between {
            expr,
            low,
            high,
            negated: false,
        } => {
            if let (Expr::Column { name, .. }, Expr::Literal(lv), Expr::Literal(hv)) =
                (expr.as_ref(), low.as_ref(), high.as_ref())
            {
                if !lv.is_null() && !hv.is_null() {
                    let entry = map.entry(name.to_ascii_lowercase()).or_default();
                    entry.tighten_lo(RangeBound::Included(lv.clone()));
                    entry.tighten_hi(RangeBound::Included(hv.clone()));
                }
            }
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            if let Expr::Column { name, .. } = expr.as_ref() {
                let mut vals = Vec::new();
                let mut all_lit = true;
                for item in list {
                    if let Expr::Literal(v) = item {
                        if !v.is_null() {
                            vals.push(v.clone());
                        }
                    } else {
                        all_lit = false;
                        break;
                    }
                }
                if all_lit && !vals.is_empty() {
                    let entry = map.entry(name.to_ascii_lowercase()).or_default();
                    match &mut entry.eq_values {
                        None => entry.eq_values = Some(vals),
                        Some(existing) => {
                            existing.retain(|ev| vals.iter().any(|nv| ev.sql_cmp(nv).is_eq()));
                        }
                    }
                }
            }
        }
        _ => {}
    }
}

pub fn extract_index_candidates(expr: &Expr) -> Vec<(String, Vec<Value>)> {
    let mut out = Vec::new();
    collect_index_cands(expr, &mut out);
    out
}

fn collect_index_cands(expr: &Expr, out: &mut Vec<(String, Vec<Value>)>) {
    match expr {
        Expr::Binary {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_index_cands(left, out);
            collect_index_cands(right, out);
        }
        Expr::Binary {
            left,
            op: BinOp::Eq,
            right,
        } => match (left.as_ref(), right.as_ref()) {
            (Expr::Column { name, .. }, Expr::Literal(v))
            | (Expr::Literal(v), Expr::Column { name, .. })
                if !v.is_null() =>
            {
                out.push((name.clone(), vec![v.clone()]));
            }
            _ => {}
        },
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            if let Expr::Column { name, .. } = expr.as_ref() {
                let mut vals = Vec::new();
                let mut all_lit = true;
                for item in list {
                    if let Expr::Literal(v) = item {
                        if !v.is_null() {
                            vals.push(v.clone());
                        }
                    } else {
                        all_lit = false;
                        break;
                    }
                }
                if all_lit && !vals.is_empty() {
                    out.push((name.clone(), vals));
                }
            }
        }
        _ => {}
    }
}

pub fn collect_expr_columns(expr: &Expr, schema: &TableSchema, mask: &mut [bool]) -> Result<()> {
    match expr {
        Expr::Literal(_) => Ok(()),
        Expr::Column { name, .. } => {
            if name.eq_ignore_ascii_case("rowid") {
                return Ok(());
            }
            if let Some(idx) = schema.col_index(name) {
                mask[idx] = true;
            }
            Ok(())
        }
        Expr::UnaryNot(inner) | Expr::UnaryNeg(inner) => collect_expr_columns(inner, schema, mask),
        Expr::Binary { left, right, .. } => {
            collect_expr_columns(left, schema, mask)?;
            collect_expr_columns(right, schema, mask)
        }
        Expr::IsNull { expr, .. } => collect_expr_columns(expr, schema, mask),
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_expr_columns(expr, schema, mask)?;
            collect_expr_columns(low, schema, mask)?;
            collect_expr_columns(high, schema, mask)
        }
        Expr::InList { expr, list, .. } => {
            collect_expr_columns(expr, schema, mask)?;
            for item in list {
                collect_expr_columns(item, schema, mask)?;
            }
            Ok(())
        }
        Expr::InSubquery { expr, .. } => collect_expr_columns(expr, schema, mask),
        Expr::Case { whens, else_expr } => {
            for (c, v) in whens {
                collect_expr_columns(c, schema, mask)?;
                collect_expr_columns(v, schema, mask)?;
            }
            if let Some(e) = else_expr {
                collect_expr_columns(e, schema, mask)?;
            }
            Ok(())
        }
        Expr::Aggregate(agg) => match agg {
            AggFunc::CountStar => Ok(()),
            AggFunc::Count(e, _)
            | AggFunc::Sum(e)
            | AggFunc::Avg(e)
            | AggFunc::Min(e)
            | AggFunc::Max(e) => collect_expr_columns(e, schema, mask),
        },
        Expr::Window { func, over } => {
            match func {
                WindowFunc::Sum(e) | WindowFunc::Avg(e) => collect_expr_columns(e, schema, mask)?,
                _ => {}
            }
            for p in &over.partition_by {
                collect_expr_columns(p, schema, mask)?;
            }
            for (o, _) in &over.order_by {
                collect_expr_columns(o, schema, mask)?;
            }
            Ok(())
        }
    }
}

pub fn page_might_match(
    expr: &Expr,
    schema: &TableSchema,
    min_rowid: i64,
    max_rowid: i64,
    row_count: u32,
    zone_maps: &[ColumnZoneMap],
    blooms: &[Option<BloomFilter>],
) -> (bool, bool) {
    match expr {
        Expr::Binary {
            left,
            op: BinOp::And,
            right,
        } => {
            let (lz, lb) = page_might_match(
                left, schema, min_rowid, max_rowid, row_count, zone_maps, blooms,
            );
            if !lz || !lb {
                return (lz, lb);
            }
            page_might_match(
                right, schema, min_rowid, max_rowid, row_count, zone_maps, blooms,
            )
        }
        Expr::Binary {
            left,
            op: BinOp::Or,
            right,
        } => {
            let (lz, lb) = page_might_match(
                left, schema, min_rowid, max_rowid, row_count, zone_maps, blooms,
            );
            let (rz, rb) = page_might_match(
                right, schema, min_rowid, max_rowid, row_count, zone_maps, blooms,
            );
            (lz || rz, (lz && lb) || (rz && rb))
        }
        Expr::Binary { left, op, right } => {
            let (col_name, lit, cmp_op) = match (left.as_ref(), right.as_ref()) {
                (Expr::Column { name, .. }, Expr::Literal(v)) => (name, v, *op),
                (Expr::Literal(v), Expr::Column { name, .. }) => {
                    let flipped = match op {
                        BinOp::Eq => BinOp::Eq,
                        BinOp::Ne => BinOp::Ne,
                        BinOp::Lt => BinOp::Gt,
                        BinOp::Le => BinOp::Ge,
                        BinOp::Gt => BinOp::Lt,
                        BinOp::Ge => BinOp::Le,
                        _ => return (true, true),
                    };
                    (name, v, flipped)
                }
                _ => return (true, true),
            };
            if lit.is_null() {
                return (false, true);
            }
            let zm_ok = if col_name.eq_ignore_ascii_case("rowid") {
                cmp_bounds(cmp_op, lit, &Value::Integer(min_rowid), &Value::Integer(max_rowid))
            } else if let Some(idx) = schema.col_index(col_name) {
                match zone_maps.get(idx).and_then(|zm| {
                    zm.min_val.as_ref().zip(zm.max_val.as_ref())
                }) {
                    Some((lo, hi)) => cmp_bounds(cmp_op, lit, lo, hi),
                    None => true,
                }
            } else {
                true
            };
            if !zm_ok {
                return (false, true);
            }
            if cmp_op == BinOp::Eq {
                if let Some(idx) = schema.col_index(col_name) {
                    if let Some(Some(bf)) = blooms.get(idx) {
                        if !bf.might_contain(lit) {
                            return (true, false);
                        }
                    }
                }
            }
            (true, true)
        }
        Expr::Between {
            expr,
            low,
            high,
            negated: false,
        } => {
            if let (Expr::Column { name, .. }, Expr::Literal(lv), Expr::Literal(hv)) =
                (expr.as_ref(), low.as_ref(), high.as_ref())
            {
                if lv.is_null() || hv.is_null() {
                    return (false, true);
                }
                let bounds = if name.eq_ignore_ascii_case("rowid") {
                    Some((Value::Integer(min_rowid), Value::Integer(max_rowid)))
                } else if let Some(idx) = schema.col_index(name) {
                    zone_maps
                        .get(idx)
                        .and_then(|zm| zm.min_val.clone().zip(zm.max_val.clone()))
                } else {
                    None
                };
                if let Some((lo, hi)) = bounds {
                    if hi.sql_cmp(lv).is_lt() || lo.sql_cmp(hv).is_gt() {
                        return (false, true);
                    }
                }
            }
            (true, true)
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            if let Expr::Column { name, .. } = expr.as_ref() {
                if let Some(idx) = schema.col_index(name) {
                    if let Some(Some(bf)) = blooms.get(idx) {
                        let mut any_lit = false;
                        let mut all_lit = true;
                        let mut any_hit = false;
                        for item in list {
                            if let Expr::Literal(v) = item {
                                any_lit = true;
                                if bf.might_contain(v) {
                                    any_hit = true;
                                    break;
                                }
                            } else {
                                all_lit = false;
                                break;
                            }
                        }
                        if all_lit && any_lit && !any_hit {
                            return (true, false);
                        }
                    }
                }
            }
            (true, true)
        }
        Expr::IsNull { expr, negated } => {
            if let Expr::Column { name, .. } = expr.as_ref() {
                if let Some(idx) = schema.col_index(name) {
                    if let Some(zm) = zone_maps.get(idx) {
                        if !negated && zm.null_count == 0 {
                            return (false, true);
                        }
                        if *negated && zm.null_count == row_count {
                            return (false, true);
                        }
                    }
                }
            }
            (true, true)
        }
        _ => (true, true),
    }
}

fn cmp_bounds(op: BinOp, lit: &Value, lo: &Value, hi: &Value) -> bool {
    match op {
        BinOp::Eq => !lit.sql_cmp(lo).is_lt() && !lit.sql_cmp(hi).is_gt(),
        BinOp::Lt => lo.sql_cmp(lit).is_lt(),
        BinOp::Le => !lo.sql_cmp(lit).is_gt(),
        BinOp::Gt => hi.sql_cmp(lit).is_gt(),
        BinOp::Ge => !hi.sql_cmp(lit).is_lt(),
        _ => true,
    }
}

// ============================================================================
// Multi-Table Row Context & Zero-Allocation Expression Evaluator
// ============================================================================

#[derive(Debug, Clone)]
pub struct EvalSchema {
    cols: Vec<(String, String)>,
}

impl EvalSchema {
    pub fn from_single(alias: &str, schema: &TableSchema) -> Self {
        let cols = schema
            .columns
            .iter()
            .map(|c| (alias.to_string(), c.name.clone()))
            .collect();
        EvalSchema { cols }
    }

    fn from_names(alias: &str, names: &[String]) -> Self {
        let cols = names
            .iter()
            .map(|n| (alias.to_string(), n.clone()))
            .collect();
        EvalSchema { cols }
    }

    fn extend(&mut self, alias: &str, names: &[String]) {
        for n in names {
            self.cols.push((alias.to_string(), n.clone()));
        }
    }

    fn has_alias(&self, alias: &str) -> bool {
        self.cols.iter().any(|(ta, _)| ta.eq_ignore_ascii_case(alias))
    }

    #[inline]
    fn resolve(&self, table: Option<&str>, name: &str) -> Result<usize> {
        if let Some(t) = table {
            let mut found: Option<usize> = None;
            for (idx, (ta, cn)) in self.cols.iter().enumerate() {
                if ta.eq_ignore_ascii_case(t) && cn.eq_ignore_ascii_case(name) {
                    if found.is_some() {
                        return Err(Error::Sql(format!("Ambiguous column '{}.{}'", t, name)));
                    }
                    found = Some(idx);
                }
            }
            found.ok_or_else(|| Error::Sql(format!("Unknown column '{}.{}'", t, name)))
        } else {
            let mut found: Option<usize> = None;
            for (idx, (_, cn)) in self.cols.iter().enumerate() {
                if cn.eq_ignore_ascii_case(name) {
                    if found.is_some() {
                        return Err(Error::Sql(format!("Ambiguous column '{}'", name)));
                    }
                    found = Some(idx);
                }
            }
            found.ok_or_else(|| Error::Sql(format!("Unknown column '{}'", name)))
        }
    }
}

fn substitute_select_aliases(expr: &Expr, items: &[SelectItem], schema: &EvalSchema) -> Expr {
    match expr {
        Expr::Column { table: None, name } => {
            if schema.resolve(None, name).is_err() {
                for item in items {
                    if let SelectItem::Expr {
                        expr: item_expr,
                        alias: Some(a),
                    } = item
                    {
                        if a.eq_ignore_ascii_case(name) {
                            return item_expr.clone();
                        }
                    }
                }
            }
            expr.clone()
        }
        Expr::UnaryNot(e) => {
            Expr::UnaryNot(Box::new(substitute_select_aliases(e, items, schema)))
        }
        Expr::UnaryNeg(e) => {
            Expr::UnaryNeg(Box::new(substitute_select_aliases(e, items, schema)))
        }
        Expr::IsNull { expr: e, negated } => Expr::IsNull {
            expr: Box::new(substitute_select_aliases(e, items, schema)),
            negated: *negated,
        },
        Expr::Between {
            expr: e,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: Box::new(substitute_select_aliases(e, items, schema)),
            low: Box::new(substitute_select_aliases(low, items, schema)),
            high: Box::new(substitute_select_aliases(high, items, schema)),
            negated: *negated,
        },
        Expr::Binary { left, op, right } => Expr::Binary {
            left: Box::new(substitute_select_aliases(left, items, schema)),
            op: *op,
            right: Box::new(substitute_select_aliases(right, items, schema)),
        },
        Expr::InList {
            expr: e,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(substitute_select_aliases(e, items, schema)),
            list: list
                .iter()
                .map(|x| substitute_select_aliases(x, items, schema))
                .collect(),
            negated: *negated,
        },
        Expr::Case { whens, else_expr } => Expr::Case {
            whens: whens
                .iter()
                .map(|(c, v)| {
                    (
                        substitute_select_aliases(c, items, schema),
                        substitute_select_aliases(v, items, schema),
                    )
                })
                .collect(),
            else_expr: else_expr
                .as_deref()
                .map(|e| Box::new(substitute_select_aliases(e, items, schema))),
        },
        other => other.clone(),
    }
}

fn validate_where_columns(expr: &Expr, schema: &EvalSchema) -> Result<()> {
    match expr {
        Expr::Literal(_) => Ok(()),
        Expr::Column { table, name } => {
            if table.is_none() && name.eq_ignore_ascii_case("rowid") {
                return Ok(());
            }
            schema.resolve(table.as_deref(), name)?;
            Ok(())
        }
        Expr::UnaryNot(e) | Expr::UnaryNeg(e) | Expr::IsNull { expr: e, .. } => {
            validate_where_columns(e, schema)
        }
        Expr::Binary { left, right, .. } => {
            validate_where_columns(left, schema)?;
            validate_where_columns(right, schema)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            validate_where_columns(expr, schema)?;
            validate_where_columns(low, schema)?;
            validate_where_columns(high, schema)
        }
        Expr::InList { expr, list, .. } => {
            validate_where_columns(expr, schema)?;
            for item in list {
                validate_where_columns(item, schema)?;
            }
            Ok(())
        }
        Expr::Case { whens, else_expr } => {
            for (c, v) in whens {
                validate_where_columns(c, schema)?;
                validate_where_columns(v, schema)?;
            }
            if let Some(e) = else_expr {
                validate_where_columns(e, schema)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

pub fn eval_expr(expr: &Expr, schema: &TableSchema, row: Option<&Row>) -> Result<Value> {
    let es = EvalSchema::from_single(&schema.name, schema);
    let vals = row.map(|r| r.values.as_slice());
    let rid = row.map(|r| r.rowid);
    eval_expr_ctx(expr, &es, vals, rid, None)
}

pub fn eval_bool(expr: &Expr, schema: &TableSchema, row: Option<&Row>) -> Result<Option<bool>> {
    value_to_bool(eval_expr(expr, schema, row)?)
}

#[inline]
pub fn eval_bool_fast(expr: &Expr, es: &EvalSchema, row: &Row) -> Result<Option<bool>> {
    value_to_bool(eval_expr_ctx(expr, es, Some(&row.values), Some(row.rowid), None)?)
}

fn value_to_bool(v: Value) -> Result<Option<bool>> {
    match v {
        Value::Null => Ok(None),
        Value::Integer(i) => Ok(Some(i != 0)),
        Value::Real(f) => Ok(Some(f != 0.0)),
        Value::Text(s) => Ok(Some(!s.is_empty())),
        Value::Blob(b) => Ok(Some(!b.is_empty())),
    }
}

fn eval_expr_ctx(
    expr: &Expr,
    schema: &EvalSchema,
    row: Option<&[Value]>,
    rowid: Option<i64>,
    group_rows: Option<&[Vec<Value>]>,
) -> Result<Value> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Column { table, name } => {
            if table.is_none() && name.eq_ignore_ascii_case("rowid") {
                if let Some(id) = rowid {
                    return Ok(Value::Integer(id));
                }
            }
            let r = row.ok_or_else(|| {
                Error::Sql(format!("Column '{}' not allowed in constant context", name))
            })?;
            let idx = schema.resolve(table.as_deref(), name)?;
            Ok(r[idx].clone())
        }
        Expr::Aggregate(agg) => {
            let rows = group_rows
                .ok_or_else(|| Error::Sql("Aggregate used outside aggregate context".into()))?;
            eval_aggregate(agg, schema, rows)
        }
        Expr::Window { .. } => Err(Error::Sql(
            "Window function only allowed in SELECT projection".into(),
        )),
        Expr::InSubquery { .. } => Err(Error::Sql(
            "Unresolved IN (SELECT ...) subquery".into(),
        )),
        Expr::UnaryNeg(inner) => match eval_expr_ctx(inner, schema, row, rowid, group_rows)? {
            Value::Null => Ok(Value::Null),
            Value::Integer(v) => Ok(Value::Integer(v.wrapping_neg())),
            Value::Real(v) => Ok(Value::Real(-v)),
            other => Err(Error::Sql(format!("Cannot negate {:?}", other))),
        },
        Expr::UnaryNot(inner) => {
            match value_to_bool(eval_expr_ctx(inner, schema, row, rowid, group_rows)?)? {
                None => Ok(Value::Null),
                Some(b) => Ok(Value::Integer(if b { 0 } else { 1 })),
            }
        }
        Expr::IsNull { expr, negated } => {
            let v = eval_expr_ctx(expr, schema, row, rowid, group_rows)?;
            let res = if *negated { !v.is_null() } else { v.is_null() };
            Ok(Value::Integer(if res { 1 } else { 0 }))
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let v = eval_expr_ctx(expr, schema, row, rowid, group_rows)?;
            let lo = eval_expr_ctx(low, schema, row, rowid, group_rows)?;
            let hi = eval_expr_ctx(high, schema, row, rowid, group_rows)?;
            let ge_lo: Option<bool> = if v.is_null() || lo.is_null() {
                None
            } else {
                Some(!v.sql_cmp(&lo).is_lt())
            };
            let le_hi: Option<bool> = if v.is_null() || hi.is_null() {
                None
            } else {
                Some(!v.sql_cmp(&hi).is_gt())
            };
            let inside: Option<bool> = match (ge_lo, le_hi) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            };
            let res = if *negated {
                inside.map(|b| !b)
            } else {
                inside
            };
            match res {
                None => Ok(Value::Null),
                Some(b) => Ok(Value::Integer(if b { 1 } else { 0 })),
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let target = eval_expr_ctx(expr, schema, row, rowid, group_rows)?;
            if list.is_empty() {
                return Ok(Value::Integer(if *negated { 1 } else { 0 }));
            }
            if target.is_null() {
                return Ok(Value::Null);
            }
            let mut found = false;
            let mut saw_null = false;
            for item in list {
                let v = eval_expr_ctx(item, schema, row, rowid, group_rows)?;
                if v.is_null() {
                    saw_null = true;
                } else if target.sql_cmp(&v) == Ordering::Equal {
                    found = true;
                    break;
                }
            }
            if found {
                Ok(Value::Integer(if *negated { 0 } else { 1 }))
            } else if saw_null {
                Ok(Value::Null)
            } else {
                Ok(Value::Integer(if *negated { 1 } else { 0 }))
            }
        }
        Expr::Case { whens, else_expr } => {
            for (cond, val) in whens {
                if value_to_bool(eval_expr_ctx(cond, schema, row, rowid, group_rows)?)?
                    == Some(true)
                {
                    return eval_expr_ctx(val, schema, row, rowid, group_rows);
                }
            }
            if let Some(e) = else_expr {
                eval_expr_ctx(e, schema, row, rowid, group_rows)
            } else {
                Ok(Value::Null)
            }
        }
        Expr::Binary { left, op, right } => match op {
            BinOp::And => {
                let l = value_to_bool(eval_expr_ctx(left, schema, row, rowid, group_rows)?)?;
                if l == Some(false) {
                    return Ok(Value::Integer(0));
                }
                let r = value_to_bool(eval_expr_ctx(right, schema, row, rowid, group_rows)?)?;
                match (l, r) {
                    (Some(true), Some(true)) => Ok(Value::Integer(1)),
                    (_, Some(false)) => Ok(Value::Integer(0)),
                    _ => Ok(Value::Null),
                }
            }
            BinOp::Or => {
                let l = value_to_bool(eval_expr_ctx(left, schema, row, rowid, group_rows)?)?;
                if l == Some(true) {
                    return Ok(Value::Integer(1));
                }
                let r = value_to_bool(eval_expr_ctx(right, schema, row, rowid, group_rows)?)?;
                match (l, r) {
                    (_, Some(true)) => Ok(Value::Integer(1)),
                    (Some(false), Some(false)) => Ok(Value::Integer(0)),
                    _ => Ok(Value::Null),
                }
            }
            BinOp::Like => {
                let lv = eval_expr_ctx(left, schema, row, rowid, group_rows)?;
                let rv = eval_expr_ctx(right, schema, row, rowid, group_rows)?;
                match (lv, rv) {
                    (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                    (Value::Text(s), Value::Text(pat)) => {
                        Ok(Value::Integer(if sql_like(&s, &pat) { 1 } else { 0 }))
                    }
                    _ => Ok(Value::Integer(0)),
                }
            }
            BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div => {
                let lv = eval_expr_ctx(left, schema, row, rowid, group_rows)?;
                let rv = eval_expr_ctx(right, schema, row, rowid, group_rows)?;
                eval_arith(lv, rv, *op)
            }
            BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => {
                let lv = eval_expr_ctx(left, schema, row, rowid, group_rows)?;
                let rv = eval_expr_ctx(right, schema, row, rowid, group_rows)?;
                if lv.is_null() || rv.is_null() {
                    return Ok(Value::Null);
                }
                let ord = lv.sql_cmp(&rv);
                let b = match op {
                    BinOp::Eq => ord == Ordering::Equal,
                    BinOp::Ne => ord != Ordering::Equal,
                    BinOp::Lt => ord == Ordering::Less,
                    BinOp::Le => ord != Ordering::Greater,
                    BinOp::Gt => ord == Ordering::Greater,
                    BinOp::Ge => ord != Ordering::Less,
                    _ => unreachable!(),
                };
                Ok(Value::Integer(if b { 1 } else { 0 }))
            }
        },
    }
}

fn eval_aggregate(agg: &AggFunc, schema: &EvalSchema, rows: &[Vec<Value>]) -> Result<Value> {
    match agg {
        AggFunc::CountStar => Ok(Value::Integer(rows.len() as i64)),
        AggFunc::Count(expr, distinct) => {
            if !distinct {
                let mut n = 0i64;
                for r in rows {
                    if !eval_expr_ctx(expr, schema, Some(r), None, None)?.is_null() {
                        n += 1;
                    }
                }
                Ok(Value::Integer(n))
            } else {
                let mut buckets: HashMap<u64, Vec<Value>> = HashMap::new();
                let mut count = 0i64;
                for r in rows {
                    let v = eval_expr_ctx(expr, schema, Some(r), None, None)?;
                    if let Some(h) = hash_value(&v) {
                        let bucket = buckets.entry(h).or_default();
                        if !bucket.iter().any(|s| s.sql_cmp(&v) == Ordering::Equal) {
                            bucket.push(v);
                            count += 1;
                        }
                    }
                }
                Ok(Value::Integer(count))
            }
        }
        AggFunc::Sum(expr) => {
            let mut has_val = false;
            let mut is_real = false;
            let mut sum_i = 0i64;
            let mut sum_f = 0.0f64;
            for r in rows {
                match eval_expr_ctx(expr, schema, Some(r), None, None)? {
                    Value::Null => {}
                    Value::Integer(i) => {
                        has_val = true;
                        sum_i = sum_i.wrapping_add(i);
                        sum_f += i as f64;
                    }
                    Value::Real(f) => {
                        has_val = true;
                        is_real = true;
                        sum_f += f;
                    }
                    other => {
                        return Err(Error::Sql(format!(
                            "Cannot SUM non-numeric value {:?}",
                            other
                        )))
                    }
                }
            }
            if !has_val {
                Ok(Value::Null)
            } else if is_real {
                Ok(Value::Real(sum_f))
            } else {
                Ok(Value::Integer(sum_i))
            }
        }
        AggFunc::Avg(expr) => {
            let mut cnt = 0u64;
            let mut sum = 0.0f64;
            for r in rows {
                match eval_expr_ctx(expr, schema, Some(r), None, None)? {
                    Value::Null => {}
                    Value::Integer(i) => {
                        cnt += 1;
                        sum += i as f64;
                    }
                    Value::Real(f) => {
                        cnt += 1;
                        sum += f;
                    }
                    other => {
                        return Err(Error::Sql(format!(
                            "Cannot AVG non-numeric value {:?}",
                            other
                        )))
                    }
                }
            }
            if cnt == 0 {
                Ok(Value::Null)
            } else {
                Ok(Value::Real(sum / (cnt as f64)))
            }
        }
        AggFunc::Min(expr) => {
            let mut best: Option<Value> = None;
            for r in rows {
                let v = eval_expr_ctx(expr, schema, Some(r), None, None)?;
                if v.is_null() {
                    continue;
                }
                match &best {
                    None => best = Some(v),
                    Some(cur) if v.sql_cmp(cur).is_lt() => best = Some(v),
                    _ => {}
                }
            }
            Ok(best.unwrap_or(Value::Null))
        }
        AggFunc::Max(expr) => {
            let mut best: Option<Value> = None;
            for r in rows {
                let v = eval_expr_ctx(expr, schema, Some(r), None, None)?;
                if v.is_null() {
                    continue;
                }
                match &best {
                    None => best = Some(v),
                    Some(cur) if v.sql_cmp(cur).is_gt() => best = Some(v),
                    _ => {}
                }
            }
            Ok(best.unwrap_or(Value::Null))
        }
    }
}

fn expr_contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate(_) => true,
        Expr::Literal(_) | Expr::Column { .. } | Expr::Window { .. } => false,
        Expr::UnaryNot(e) | Expr::UnaryNeg(e) | Expr::IsNull { expr: e, .. } => {
            expr_contains_aggregate(e)
        }
        Expr::Binary { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_aggregate(expr)
                || expr_contains_aggregate(low)
                || expr_contains_aggregate(high)
        }
        Expr::InList { expr, list, .. } => {
            expr_contains_aggregate(expr) || list.iter().any(expr_contains_aggregate)
        }
        Expr::InSubquery { expr, .. } => expr_contains_aggregate(expr),
        Expr::Case { whens, else_expr } => {
            whens
                .iter()
                .any(|(c, v)| expr_contains_aggregate(c) || expr_contains_aggregate(v))
                || else_expr
                    .as_deref()
                    .map(expr_contains_aggregate)
                    .unwrap_or(false)
        }
    }
}

fn default_expr_label(expr: &Expr) -> String {
    match expr {
        Expr::Column { name, .. } => name.clone(),
        Expr::Aggregate(AggFunc::CountStar) => "COUNT(*)".into(),
        Expr::Aggregate(AggFunc::Count(e, d)) => {
            if *d {
                format!("COUNT(DISTINCT {})", default_expr_label(e))
            } else {
                format!("COUNT({})", default_expr_label(e))
            }
        }
        Expr::Aggregate(AggFunc::Sum(e)) => format!("SUM({})", default_expr_label(e)),
        Expr::Aggregate(AggFunc::Avg(e)) => format!("AVG({})", default_expr_label(e)),
        Expr::Aggregate(AggFunc::Min(e)) => format!("MIN({})", default_expr_label(e)),
        Expr::Aggregate(AggFunc::Max(e)) => format!("MAX({})", default_expr_label(e)),
        Expr::Window { func, .. } => match func {
            WindowFunc::RowNumber => "ROW_NUMBER()".into(),
            WindowFunc::Rank => "RANK()".into(),
            WindowFunc::CountStar => "COUNT(*) OVER".into(),
            WindowFunc::Sum(e) => format!("SUM({}) OVER", default_expr_label(e)),
            WindowFunc::Avg(e) => format!("AVG({}) OVER", default_expr_label(e)),
        },
        _ => "expr".into(),
    }
}

fn eval_arith(lv: Value, rv: Value, op: BinOp) -> Result<Value> {
    if lv.is_null() || rv.is_null() {
        return Ok(Value::Null);
    }
    match (lv, rv) {
        (Value::Integer(a), Value::Integer(b)) => match op {
            BinOp::Add => Ok(Value::Integer(a.wrapping_add(b))),
            BinOp::Sub => Ok(Value::Integer(a.wrapping_sub(b))),
            BinOp::Mul => Ok(Value::Integer(a.wrapping_mul(b))),
            BinOp::Div => {
                if b == 0 {
                    Ok(Value::Null)
                } else if a == i64::MIN && b == -1 {
                    Err(Error::Sql("Integer overflow in division".into()))
                } else {
                    Ok(Value::Integer(a / b))
                }
            }
            _ => unreachable!(),
        },
        (a, b) => {
            let fa = match a {
                Value::Integer(i) => i as f64,
                Value::Real(f) => f,
                other => return Err(Error::Sql(format!("Non-numeric operand: {:?}", other))),
            };
            let fb = match b {
                Value::Integer(i) => i as f64,
                Value::Real(f) => f,
                other => return Err(Error::Sql(format!("Non-numeric operand: {:?}", other))),
            };
            match op {
                BinOp::Add => Ok(Value::Real(fa + fb)),
                BinOp::Sub => Ok(Value::Real(fa - fb)),
                BinOp::Mul => Ok(Value::Real(fa * fb)),
                BinOp::Div => {
                    if fb == 0.0 {
                        Ok(Value::Null)
                    } else {
                        Ok(Value::Real(fa / fb))
                    }
                }
                _ => unreachable!(),
            }
        }
    }
}

fn sql_like(text: &str, pattern: &str) -> bool {
    let t: Vec<char> = text.to_lowercase().chars().collect();
    let p: Vec<char> = pattern.to_lowercase().chars().collect();
    let (mut ti, mut pi) = (0usize, 0usize);
    let mut star_idx: Option<usize> = None;
    let mut match_idx = 0usize;

    while ti < t.len() {
        if pi < p.len() && (p[pi] == '_' || p[pi] == t[ti]) {
            ti += 1;
            pi += 1;
        } else if pi < p.len() && p[pi] == '%' {
            star_idx = Some(pi);
            match_idx = ti;
            pi += 1;
        } else if let Some(si) = star_idx {
            pi = si + 1;
            match_idx += 1;
            ti = match_idx;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '%' {
        pi += 1;
    }
    pi == p.len()
}

#[derive(Debug, Clone)]
struct GroupKey(Vec<Value>);

impl PartialEq for GroupKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.len() == other.0.len()
            && self
                .0
                .iter()
                .zip(&other.0)
                .all(|(a, b)| a.sql_cmp(b) == Ordering::Equal)
    }
}
impl Eq for GroupKey {}
impl std::hash::Hash for GroupKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for v in &self.0 {
            match hash_value(v) {
                Some(h) => {
                    1u8.hash(state);
                    h.hash(state);
                }
                None => 0u8.hash(state),
            }
        }
    }
}

// ============================================================================
// Vectorized Filter & GROUP BY Planner (fast path!)
// ============================================================================

#[inline]
fn alias_matches(table: &Option<String>, base_alias: &str) -> bool {
    match table {
        None => true,
        Some(t) => t.eq_ignore_ascii_case(base_alias),
    }
}

fn compile_vec_filter(expr: &Expr, schema: &TableSchema, base_alias: &str) -> Option<VecFilter> {
    match expr {
        Expr::Binary {
            left,
            op: BinOp::And,
            right,
        } => Some(VecFilter::And(
            Box::new(compile_vec_filter(left, schema, base_alias)?),
            Box::new(compile_vec_filter(right, schema, base_alias)?),
        )),
        Expr::Binary { left, op, right } => {
            let (col_name, lit, cmp) = match (left.as_ref(), right.as_ref()) {
                (Expr::Column { table, name }, Expr::Literal(v))
                    if alias_matches(table, base_alias) =>
                {
                    (name, v, *op)
                }
                (Expr::Literal(v), Expr::Column { table, name })
                    if alias_matches(table, base_alias) =>
                {
                    let fl = match op {
                        BinOp::Eq => BinOp::Eq,
                        BinOp::Lt => BinOp::Gt,
                        BinOp::Le => BinOp::Ge,
                        BinOp::Gt => BinOp::Lt,
                        BinOp::Ge => BinOp::Le,
                        _ => return None,
                    };
                    (name, v, fl)
                }
                _ => return None,
            };
            let ci = schema.col_index(col_name)?;
            match (schema.columns[ci].col_type, cmp, lit) {
                (ColumnType::Text, BinOp::Eq, Value::Text(s)) => {
                    Some(VecFilter::EqStr(ci, s.clone()))
                }
                (ColumnType::Integer, BinOp::Eq, Value::Integer(v)) => {
                    Some(VecFilter::EqInt(ci, *v))
                }
                (ColumnType::Integer, BinOp::Gt, Value::Integer(v)) => {
                    Some(VecFilter::GtInt(ci, *v))
                }
                (ColumnType::Integer, BinOp::Ge, Value::Integer(v)) => {
                    Some(VecFilter::GeInt(ci, *v))
                }
                (ColumnType::Integer, BinOp::Lt, Value::Integer(v)) => {
                    Some(VecFilter::LtInt(ci, *v))
                }
                (ColumnType::Integer, BinOp::Le, Value::Integer(v)) => {
                    Some(VecFilter::LeInt(ci, *v))
                }
                (ColumnType::Real, BinOp::Gt, Value::Real(v)) if !v.is_nan() => {
                    Some(VecFilter::GtReal(ci, *v))
                }
                (ColumnType::Real, BinOp::Gt, Value::Integer(v)) if (*v as f64) as i64 == *v => {
                    Some(VecFilter::GtReal(ci, *v as f64))
                }
                (ColumnType::Real, BinOp::Lt, Value::Real(v)) if !v.is_nan() => {
                    Some(VecFilter::LtReal(ci, *v))
                }
                (ColumnType::Real, BinOp::Lt, Value::Integer(v)) if (*v as f64) as i64 == *v => {
                    Some(VecFilter::LtReal(ci, *v as f64))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Attempts to run a single-table GROUP BY query through the zero-transposition vectorized batch engine!
fn try_vectorized_group_by(
    db: &mut Database,
    sel: &SelectStmt,
    base_schema: &TableSchema,
    base_alias: &str,
) -> Result<Option<(Vec<String>, Vec<Vec<Value>>, String)>> {
    if !sel.joins.is_empty() || sel.group_by.len() > 1 {
        return Ok(None);
    }

    // Group column must be None (global) or a single Text/Int column
    let group_col = if sel.group_by.is_empty() {
        None
    } else if let Expr::Column { table, name } = &sel.group_by[0] {
        if !alias_matches(table, base_alias) {
            return Ok(None);
        }
        let ci = match base_schema.col_index(name) {
            Some(i) => i,
            None => return Ok(None),
        };
        if !matches!(
            base_schema.columns[ci].col_type,
            ColumnType::Text | ColumnType::Integer
        ) {
            return Ok(None);
        }
        Some(ci)
    } else {
        return Ok(None);
    };

    // Optional WHERE must compile cleanly to VecFilter
    let vec_filter = match &sel.where_clause {
        None => None,
        Some(w) => match compile_vec_filter(w, base_schema, base_alias) {
            Some(vf) => Some(vf),
            None => return Ok(None),
        },
    };

    // Every SELECT item must be either the group_by column or a supported direct aggregate
    let mut agg_kinds: Vec<VecAggKind> = Vec::new();
    let mut item_plan: Vec<Option<usize>> = Vec::new(); // None = group key, Some(agg_idx)
    let mut out_cols = Vec::new();
    let mut needed = vec![false; base_schema.columns.len()];

    if let Some(gc) = group_col {
        needed[gc] = true;
    }
    if let Some(ref w) = sel.where_clause {
        collect_expr_columns(w, base_schema, &mut needed)?;
    }

    for item in &sel.items {
        let (expr, alias) = match item {
            SelectItem::Expr { expr, alias } => (expr, alias),
            SelectItem::Wildcard(_) => return Ok(None),
        };
        out_cols.push(alias.clone().unwrap_or_else(|| default_expr_label(expr)));
        match expr {
            Expr::Column { table, name } => {
                if !alias_matches(table, base_alias) {
                    return Ok(None);
                }
                let ci = match base_schema.col_index(name) {
                    Some(i) => i,
                    None => return Ok(None),
                };
                if Some(ci) != group_col {
                    return Ok(None);
                }
                item_plan.push(None);
            }
            Expr::Aggregate(agg) => {
                let kind = match agg {
                    AggFunc::CountStar => VecAggKind::CountStar,
                    AggFunc::Count(e, false) => {
                        if let Expr::Column { table, name } = e.as_ref() {
                            if !alias_matches(table, base_alias) {
                                return Ok(None);
                            }
                            let ci = match base_schema.col_index(name) {
                                Some(i) => i,
                                None => return Ok(None),
                            };
                            needed[ci] = true;
                            VecAggKind::Count(ci)
                        } else {
                            return Ok(None);
                        }
                    }
                    AggFunc::Sum(e) => {
                        if let Expr::Column { table, name } = e.as_ref() {
                            if !alias_matches(table, base_alias) {
                                return Ok(None);
                            }
                            let ci = match base_schema.col_index(name) {
                                Some(i) => i,
                                None => return Ok(None),
                            };
                            if !matches!(
                                base_schema.columns[ci].col_type,
                                ColumnType::Integer | ColumnType::Real
                            ) {
                                return Ok(None);
                            }
                            needed[ci] = true;
                            VecAggKind::Sum(ci)
                        } else {
                            return Ok(None);
                        }
                    }
                    AggFunc::Avg(e) => {
                        if let Expr::Column { table, name } = e.as_ref() {
                            if !alias_matches(table, base_alias) {
                                return Ok(None);
                            }
                            let ci = match base_schema.col_index(name) {
                                Some(i) => i,
                                None => return Ok(None),
                            };
                            if !matches!(
                                base_schema.columns[ci].col_type,
                                ColumnType::Integer | ColumnType::Real
                            ) {
                                return Ok(None);
                            }
                            needed[ci] = true;
                            VecAggKind::Avg(ci)
                        } else {
                            return Ok(None);
                        }
                    }
                    AggFunc::Min(e) => {
                        if let Expr::Column { table, name } = e.as_ref() {
                            if !alias_matches(table, base_alias) {
                                return Ok(None);
                            }
                            let ci = match base_schema.col_index(name) {
                                Some(i) => i,
                                None => return Ok(None),
                            };
                            needed[ci] = true;
                            VecAggKind::Min(ci)
                        } else {
                            return Ok(None);
                        }
                    }
                    AggFunc::Max(e) => {
                        if let Expr::Column { table, name } = e.as_ref() {
                            if !alias_matches(table, base_alias) {
                                return Ok(None);
                            }
                            let ci = match base_schema.col_index(name) {
                                Some(i) => i,
                                None => return Ok(None),
                            };
                            needed[ci] = true;
                            VecAggKind::Max(ci)
                        } else {
                            return Ok(None);
                        }
                    }
                    _ => return Ok(None),
                };
                let ai = agg_kinds.len();
                agg_kinds.push(kind);
                item_plan.push(Some(ai));
            }
            _ => return Ok(None),
        }
    }

    // If HAVING is present, allow simple `COUNT(*) op lit` or `alias op lit` for comparison ops only
    let having_agg_idx: Option<(usize, BinOp, Value)> = if let Some(ref h) = sel.having {
        if let Expr::Binary { left, op, right } = h {
            if !matches!(
                op,
                BinOp::Eq | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge
            ) {
                return Ok(None);
            }
            if let Expr::Literal(lit) = right.as_ref() {
                match left.as_ref() {
                    Expr::Aggregate(AggFunc::CountStar) => {
                        let ai = agg_kinds.len();
                        agg_kinds.push(VecAggKind::CountStar);
                        Some((ai, *op, lit.clone()))
                    }
                    Expr::Column { table: None, name } => {
                        if let Some(pos) = out_cols.iter().position(|c| c.eq_ignore_ascii_case(name))
                        {
                            if let Some(ai) = item_plan[pos] {
                                Some((ai, *op, lit.clone()))
                            } else {
                                return Ok(None);
                            }
                        } else {
                            return Ok(None);
                        }
                    }
                    _ => return Ok(None),
                }
            } else {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }
    } else {
        None
    };

    let (groups, m) = db.vectorized_group_by(
        &base_schema.name,
        group_col,
        &agg_kinds,
        vec_filter.as_ref(),
        sel.where_clause.as_ref(),
        &needed,
    )?;

    let mut out_rows: Vec<(Vec<Value>, Vec<Value>)> = Vec::with_capacity(groups.len());
    for (gkey, agg_vals) in groups {
        if let Some((ai, op, ref lit)) = having_agg_idx {
            let v = &agg_vals[ai];
            if v.is_null() || lit.is_null() {
                continue;
            }
            let ord = v.sql_cmp(lit);
            let ok = match op {
                BinOp::Eq => ord == Ordering::Equal,
                BinOp::Ne => ord != Ordering::Equal,
                BinOp::Lt => ord == Ordering::Less,
                BinOp::Le => ord != Ordering::Greater,
                BinOp::Gt => ord == Ordering::Greater,
                BinOp::Ge => ord != Ordering::Less,
                _ => false,
            };
            if !ok {
                continue;
            }
        }
        let mut proj = Vec::with_capacity(item_plan.len());
        for p in &item_plan {
            match p {
                None => proj.push(gkey.clone().unwrap_or(Value::Null)),
                Some(ai) => proj.push(agg_vals[*ai].clone()),
            }
        }
        let mut skeys = Vec::with_capacity(sel.order_by.len());
        for (oexpr, _) in &sel.order_by {
            if let Expr::Column { table: None, name } = oexpr {
                if let Some(pos) = out_cols.iter().position(|c| c.eq_ignore_ascii_case(name)) {
                    skeys.push(proj[pos].clone());
                    continue;
                }
            }
            return Ok(None);
        }
        out_rows.push((proj, skeys));
    }

    if !sel.order_by.is_empty() {
        let dirs: Vec<bool> = sel.order_by.iter().map(|(_, asc)| *asc).collect();
        out_rows.sort_by(|a, b| {
            for (i, &asc) in dirs.iter().enumerate() {
                let o = a.1[i].sql_cmp(&b.1[i]);
                if o != Ordering::Equal {
                    return if asc { o } else { o.reverse() };
                }
            }
            Ordering::Equal
        });
    }

    let start = sel.offset.unwrap_or(0).min(out_rows.len());
    let end = match sel.limit {
        Some(l) => (start + l).min(out_rows.len()),
        None => out_rows.len(),
    };
    let final_rows: Vec<Vec<Value>> = out_rows
        .into_iter()
        .skip(start)
        .take(end - start)
        .map(|(p, _)| p)
        .collect();

    let parallel_note = if m.parallel_workers > 1 {
        format!(" | PARALLEL (workers={})", m.parallel_workers)
    } else {
        String::new()
    };
    let plan = format!(
        "VECTORIZED COLUMNAR SCAN + STREAMING HASH AGGREGATE '{}'{} | total_pages={} | pruned: zonemap={} bloom={} | scanned={} -> {} groups",
        base_schema.name,
        parallel_note,
        m.total_pages,
        m.pruned_by_zonemap,
        m.pruned_by_bloom,
        m.pages_scanned,
        final_rows.len()
    );
    Ok(Some((out_cols, final_rows, plan)))
}

// ============================================================================
// Subquery resolution + Window Function Evaluation + Main SELECT Executor
// ============================================================================

type VirtualTables = HashMap<String, (Vec<String>, Vec<Vec<Value>>)>;

fn resolve_subqueries_in_expr(
    db: &mut Database,
    expr: &Expr,
    ctes: &VirtualTables,
) -> Result<Expr> {
    match expr {
        Expr::InSubquery {
            expr: inner,
            subquery,
            negated,
        } => {
            let resolved_inner = resolve_subqueries_in_expr(db, inner, ctes)?;
            let (_, sub_rows, _) = execute_select_with_ctes(db, subquery, ctes)?;
            let list = sub_rows
                .into_iter()
                .map(|r| Expr::Literal(r.into_iter().next().unwrap_or(Value::Null)))
                .collect();
            Ok(Expr::InList {
                expr: Box::new(resolved_inner),
                list,
                negated: *negated,
            })
        }
        Expr::UnaryNot(e) => Ok(Expr::UnaryNot(Box::new(resolve_subqueries_in_expr(
            db, e, ctes,
        )?))),
        Expr::UnaryNeg(e) => Ok(Expr::UnaryNeg(Box::new(resolve_subqueries_in_expr(
            db, e, ctes,
        )?))),
        Expr::IsNull { expr, negated } => Ok(Expr::IsNull {
            expr: Box::new(resolve_subqueries_in_expr(db, expr, ctes)?),
            negated: *negated,
        }),
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Ok(Expr::Between {
            expr: Box::new(resolve_subqueries_in_expr(db, expr, ctes)?),
            low: Box::new(resolve_subqueries_in_expr(db, low, ctes)?),
            high: Box::new(resolve_subqueries_in_expr(db, high, ctes)?),
            negated: *negated,
        }),
        Expr::Binary { left, op, right } => Ok(Expr::Binary {
            left: Box::new(resolve_subqueries_in_expr(db, left, ctes)?),
            op: *op,
            right: Box::new(resolve_subqueries_in_expr(db, right, ctes)?),
        }),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let mut new_list = Vec::with_capacity(list.len());
            for item in list {
                new_list.push(resolve_subqueries_in_expr(db, item, ctes)?);
            }
            Ok(Expr::InList {
                expr: Box::new(resolve_subqueries_in_expr(db, expr, ctes)?),
                list: new_list,
                negated: *negated,
            })
        }
        Expr::Case { whens, else_expr } => {
            let mut new_whens = Vec::with_capacity(whens.len());
            for (c, v) in whens {
                new_whens.push((
                    resolve_subqueries_in_expr(db, c, ctes)?,
                    resolve_subqueries_in_expr(db, v, ctes)?,
                ));
            }
            let new_else = match else_expr {
                Some(e) => Some(Box::new(resolve_subqueries_in_expr(db, e, ctes)?)),
                None => None,
            };
            Ok(Expr::Case {
                whens: new_whens,
                else_expr: new_else,
            })
        }
        Expr::Aggregate(agg) => {
            let new_agg = match agg {
                AggFunc::CountStar => AggFunc::CountStar,
                AggFunc::Count(e, d) => {
                    AggFunc::Count(Box::new(resolve_subqueries_in_expr(db, e, ctes)?), *d)
                }
                AggFunc::Sum(e) => AggFunc::Sum(Box::new(resolve_subqueries_in_expr(db, e, ctes)?)),
                AggFunc::Avg(e) => AggFunc::Avg(Box::new(resolve_subqueries_in_expr(db, e, ctes)?)),
                AggFunc::Min(e) => AggFunc::Min(Box::new(resolve_subqueries_in_expr(db, e, ctes)?)),
                AggFunc::Max(e) => AggFunc::Max(Box::new(resolve_subqueries_in_expr(db, e, ctes)?)),
            };
            Ok(Expr::Aggregate(new_agg))
        }
        Expr::Window { func, over } => {
            let new_func = match func {
                WindowFunc::RowNumber => WindowFunc::RowNumber,
                WindowFunc::Rank => WindowFunc::Rank,
                WindowFunc::CountStar => WindowFunc::CountStar,
                WindowFunc::Sum(e) => {
                    WindowFunc::Sum(Box::new(resolve_subqueries_in_expr(db, e, ctes)?))
                }
                WindowFunc::Avg(e) => {
                    WindowFunc::Avg(Box::new(resolve_subqueries_in_expr(db, e, ctes)?))
                }
            };
            let mut new_part = Vec::with_capacity(over.partition_by.len());
            for p in &over.partition_by {
                new_part.push(resolve_subqueries_in_expr(db, p, ctes)?);
            }
            let mut new_ord = Vec::with_capacity(over.order_by.len());
            for (o, asc) in &over.order_by {
                new_ord.push((resolve_subqueries_in_expr(db, o, ctes)?, *asc));
            }
            Ok(Expr::Window {
                func: new_func,
                over: WindowSpec {
                    partition_by: new_part,
                    order_by: new_ord,
                },
            })
        }
        other => Ok(other.clone()),
    }
}

fn eval_window_column(
    func: &WindowFunc,
    over: &WindowSpec,
    schema: &EvalSchema,
    rows: &[(Option<i64>, Vec<Value>)],
) -> Result<Vec<Value>> {
    let n = rows.len();
    let mut result = vec![Value::Null; n];
    if n == 0 {
        return Ok(result);
    }

    // Group row indices by PARTITION BY key
    let mut partitions: HashMap<GroupKey, Vec<usize>> = HashMap::new();
    for (idx, (rid, vals)) in rows.iter().enumerate() {
        let mut pkey = Vec::with_capacity(over.partition_by.len());
        for pexpr in &over.partition_by {
            pkey.push(eval_expr_ctx(pexpr, schema, Some(vals), *rid, None)?);
        }
        partitions.entry(GroupKey(pkey)).or_default().push(idx);
    }

    for (_, mut indices) in partitions {
        // Sort partition rows by window ORDER BY
        let mut sort_keys: Vec<(usize, Vec<Value>)> = Vec::with_capacity(indices.len());
        for &ri in &indices {
            let mut ks = Vec::with_capacity(over.order_by.len());
            for (oexpr, _) in &over.order_by {
                ks.push(eval_expr_ctx(
                    oexpr,
                    schema,
                    Some(&rows[ri].1),
                    rows[ri].0,
                    None,
                )?);
            }
            sort_keys.push((ri, ks));
        }
        if !over.order_by.is_empty() {
            let dirs: Vec<bool> = over.order_by.iter().map(|(_, asc)| *asc).collect();
            sort_keys.sort_by(|a, b| {
                for (i, &asc) in dirs.iter().enumerate() {
                    let o = a.1[i].sql_cmp(&b.1[i]);
                    if o != Ordering::Equal {
                        return if asc { o } else { o.reverse() };
                    }
                }
                Ordering::Equal
            });
        }
        indices = sort_keys.iter().map(|(ri, _)| *ri).collect();

        match func {
            WindowFunc::RowNumber => {
                for (pos, &ri) in indices.iter().enumerate() {
                    result[ri] = Value::Integer((pos + 1) as i64);
                }
            }
            WindowFunc::Rank => {
                let mut rank = 1i64;
                for (pos, (ri, ks)) in sort_keys.iter().enumerate() {
                    if pos > 0 && ks.iter().zip(&sort_keys[pos - 1].1).any(|(a, b)| a.sql_cmp(b) != Ordering::Equal) {
                        rank = (pos + 1) as i64;
                    }
                    result[*ri] = Value::Integer(rank);
                }
            }
            WindowFunc::CountStar => {
                if over.order_by.is_empty() {
                    let cnt = indices.len() as i64;
                    for &ri in &indices {
                        result[ri] = Value::Integer(cnt);
                    }
                } else {
                    // SQL default frame with ORDER BY is RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (peers included together)
                    let mut start = 0usize;
                    while start < sort_keys.len() {
                        let mut end = start + 1;
                        while end < sort_keys.len()
                            && sort_keys[end]
                                .1
                                .iter()
                                .zip(&sort_keys[start].1)
                                .all(|(a, b)| a.sql_cmp(b) == Ordering::Equal)
                        {
                            end += 1;
                        }
                        for pos in start..end {
                            result[sort_keys[pos].0] = Value::Integer(end as i64);
                        }
                        start = end;
                    }
                }
            }
            WindowFunc::Sum(expr) => {
                if over.order_by.is_empty() {
                    let part_rows: Vec<Vec<Value>> =
                        indices.iter().map(|&ri| rows[ri].1.clone()).collect();
                    let total = eval_aggregate(&AggFunc::Sum(expr.clone()), schema, &part_rows)?;
                    for &ri in &indices {
                        result[ri] = total.clone();
                    }
                } else {
                    let mut sum = 0.0f64;
                    let mut has_val = false;
                    let mut is_real = false;
                    let mut sum_i = 0i64;
                    let mut start = 0usize;
                    while start < sort_keys.len() {
                        let mut end = start + 1;
                        while end < sort_keys.len()
                            && sort_keys[end]
                                .1
                                .iter()
                                .zip(&sort_keys[start].1)
                                .all(|(a, b)| a.sql_cmp(b) == Ordering::Equal)
                        {
                            end += 1;
                        }
                        for pos in start..end {
                            let ri = sort_keys[pos].0;
                            match eval_expr_ctx(expr, schema, Some(&rows[ri].1), rows[ri].0, None)?
                            {
                                Value::Null => {}
                                Value::Integer(v) => {
                                    has_val = true;
                                    sum_i = sum_i.wrapping_add(v);
                                    sum += v as f64;
                                }
                                Value::Real(v) => {
                                    has_val = true;
                                    is_real = true;
                                    sum += v;
                                }
                                other => {
                                    return Err(Error::Sql(format!(
                                        "Cannot SUM non-numeric value {:?}",
                                        other
                                    )))
                                }
                            }
                        }
                        let peer_val = if !has_val {
                            Value::Null
                        } else if is_real {
                            Value::Real(sum)
                        } else {
                            Value::Integer(sum_i)
                        };
                        for pos in start..end {
                            result[sort_keys[pos].0] = peer_val.clone();
                        }
                        start = end;
                    }
                }
            }
            WindowFunc::Avg(expr) => {
                if over.order_by.is_empty() {
                    let part_rows: Vec<Vec<Value>> =
                        indices.iter().map(|&ri| rows[ri].1.clone()).collect();
                    let avg = eval_aggregate(&AggFunc::Avg(expr.clone()), schema, &part_rows)?;
                    for &ri in &indices {
                        result[ri] = avg.clone();
                    }
                } else {
                    let mut sum = 0.0f64;
                    let mut cnt = 0u64;
                    let mut start = 0usize;
                    while start < sort_keys.len() {
                        let mut end = start + 1;
                        while end < sort_keys.len()
                            && sort_keys[end]
                                .1
                                .iter()
                                .zip(&sort_keys[start].1)
                                .all(|(a, b)| a.sql_cmp(b) == Ordering::Equal)
                        {
                            end += 1;
                        }
                        for pos in start..end {
                            let ri = sort_keys[pos].0;
                            match eval_expr_ctx(expr, schema, Some(&rows[ri].1), rows[ri].0, None)?
                            {
                                Value::Null => {}
                                Value::Integer(v) => {
                                    cnt += 1;
                                    sum += v as f64;
                                }
                                Value::Real(v) => {
                                    cnt += 1;
                                    sum += v;
                                }
                                other => {
                                    return Err(Error::Sql(format!(
                                        "Cannot AVG non-numeric value {:?}",
                                        other
                                    )))
                                }
                            }
                        }
                        let peer_val = if cnt == 0 {
                            Value::Null
                        } else {
                            Value::Real(sum / (cnt as f64))
                        };
                        for pos in start..end {
                            result[sort_keys[pos].0] = peer_val.clone();
                        }
                        start = end;
                    }
                }
            }
        }
    }
    Ok(result)
}

fn execute_select(
    db: &mut Database,
    sel: &SelectStmt,
) -> Result<(Vec<String>, Vec<Vec<Value>>, String)> {
    let empty = VirtualTables::new();
    execute_select_with_ctes(db, sel, &empty)
}

fn execute_select_with_ctes(
    db: &mut Database,
    sel: &SelectStmt,
    outer_ctes: &VirtualTables,
) -> Result<(Vec<String>, Vec<Vec<Value>>, String)> {
    // 1. Materialize any CTEs (`WITH ...`) in order
    let mut ctes = outer_ctes.clone();
    let mut plan_notes = Vec::new();
    for cte in &sel.ctes {
        let (cols, rows, _) = execute_select_with_ctes(db, &cte.query, &ctes)?;
        plan_notes.push(format!("CTE '{}' ({} rows)", cte.name, rows.len()));
        ctes.insert(cte.name.to_ascii_lowercase(), (cols, rows));
    }

    // Resolve any `IN (SELECT ...)` subqueries across WHERE, HAVING, JOIN ON, GROUP BY, ORDER BY, and SELECT items
    let mut sel_eff = sel.clone();
    sel_eff.where_clause = match &sel.where_clause {
        Some(w) => Some(resolve_subqueries_in_expr(db, w, &ctes)?),
        None => None,
    };
    sel_eff.having = match &sel.having {
        Some(h) => Some(resolve_subqueries_in_expr(db, h, &ctes)?),
        None => None,
    };
    for join in &mut sel_eff.joins {
        join.on = resolve_subqueries_in_expr(db, &join.on, &ctes)?;
    }
    for item in &mut sel_eff.items {
        if let SelectItem::Expr { expr, .. } = item {
            *expr = resolve_subqueries_in_expr(db, expr, &ctes)?;
        }
    }
    for g in &mut sel_eff.group_by {
        *g = resolve_subqueries_in_expr(db, g, &ctes)?;
    }
    for (o, _) in &mut sel_eff.order_by {
        *o = resolve_subqueries_in_expr(db, o, &ctes)?;
    }

    // 2. Check if base FROM is a real physical table (enables fast paths: COUNT(*), Vectorized GROUP BY, Index/Bloom/ZM!)
    let physical_base: Option<TableSchema> = match &sel_eff.from.factor {
        TableFactor::Table(name) if !ctes.contains_key(&name.to_ascii_lowercase()) => {
            Some(
                db.table_schema(name)
                    .ok_or_else(|| Error::Schema(format!("No such table: '{}'", name)))?
                    .clone(),
            )
        }
        _ => None,
    };

    // Fast path A: SELECT COUNT(*) FROM physical_table
    if let Some(ref base_schema) = physical_base {
        let base_alias = sel_eff.from.alias.as_deref().unwrap_or(&base_schema.name);
        if sel_eff.joins.is_empty()
            && sel_eff.where_clause.is_none()
            && sel_eff.group_by.is_empty()
            && sel_eff.having.is_none()
            && sel_eff.items.len() == 1
        {
            if let SelectItem::Expr {
                expr: Expr::Aggregate(AggFunc::CountStar),
                alias,
            } = &sel_eff.items[0]
            {
                let count = db.count_rows(&base_schema.name)?;
                let col = alias.clone().unwrap_or_else(|| "COUNT(*)".into());
                let plan = format!(
                    "FAST METADATA COUNT(*) on '{}' -> 0 pages scanned ({} rows)",
                    base_schema.name, count
                );
                let out_rows = if sel_eff.offset.unwrap_or(0) >= 1 || sel_eff.limit == Some(0) {
                    Vec::new()
                } else {
                    vec![vec![Value::Integer(count as i64)]]
                };
                return Ok((vec![col], out_rows, plan));
            }
        }

        // Fast path B: Vectorized Columnar Streaming GROUP BY!
        let has_agg = !sel_eff.group_by.is_empty()
            || sel_eff.having.is_some()
            || sel_eff.items.iter().any(|it| match it {
                SelectItem::Expr { expr, .. } => expr_contains_aggregate(expr),
                SelectItem::Wildcard(_) => false,
            });
        if has_agg && !sel_eff.distinct {
            if let Some(res) = try_vectorized_group_by(db, &sel_eff, base_schema, base_alias)? {
                return Ok(res);
            }
        }

        // Fast path C: Zero-Clone Paged Indexed Point Lookup (`SELECT cols FROM t WHERE idx_col = lit`)!
        if !has_agg
            && sel_eff.joins.is_empty()
            && !sel_eff.distinct
            && sel_eff.having.is_none()
            && sel_eff.order_by.is_empty()
            && sel_eff.offset.is_none()
            && !sel_eff
                .items
                .iter()
                .any(|it| matches!(it, SelectItem::Expr { expr: Expr::Window { .. }, .. }))
        {
            if let Some(Expr::Binary {
                left,
                op: BinOp::Eq,
                right,
            }) = &sel_eff.where_clause
            {
                let eq_target = match (left.as_ref(), right.as_ref()) {
                    (Expr::Column { table, name }, Expr::Literal(v))
                    | (Expr::Literal(v), Expr::Column { table, name })
                        if alias_matches(table, base_alias) =>
                    {
                        base_schema.col_index(name).map(|ci| (ci, v))
                    }
                    _ => None,
                };
                if let Some((filter_ci, target_val)) = eq_target {
                    let mut proj_cols: Vec<Option<usize>> = Vec::new();
                    let mut out_cols: Vec<String> = Vec::new();
                    let mut simple_proj = true;
                    for item in &sel_eff.items {
                        match item {
                            SelectItem::Wildcard(tbl_opt) => {
                                if !alias_matches(tbl_opt, base_alias) {
                                    simple_proj = false;
                                    break;
                                }
                                for (i, c) in base_schema.columns.iter().enumerate() {
                                    proj_cols.push(Some(i));
                                    out_cols.push(c.name.clone());
                                }
                            }
                            SelectItem::Expr {
                                expr: Expr::Column { table, name },
                                alias,
                            } => {
                                if !alias_matches(table, base_alias) {
                                    simple_proj = false;
                                    break;
                                }
                                if table.is_none() && name.eq_ignore_ascii_case("rowid") {
                                    proj_cols.push(None);
                                    out_cols.push(alias.clone().unwrap_or_else(|| "rowid".into()));
                                } else if let Some(ci) = base_schema.col_index(name) {
                                    proj_cols.push(Some(ci));
                                    out_cols.push(alias.clone().unwrap_or_else(|| name.clone()));
                                } else {
                                    simple_proj = false;
                                    break;
                                }
                            }
                            _ => {
                                simple_proj = false;
                                break;
                            }
                        }
                    }
                    if simple_proj {
                        if let Some((mut found_rows, m)) = db.fast_indexed_point_lookup(
                            &base_schema.name,
                            filter_ci,
                            target_val,
                            &proj_cols,
                        )? {
                            if let Some(lim) = sel_eff.limit {
                                found_rows.truncate(lim);
                            }
                            let plan = format!(
                                "FAST INDEXED POINT LOOKUP '{}' | total_pages={} | pruned: index={} | scanned={} (cache_hits={}, idx_blocks={}){}",
                                base_schema.name,
                                m.total_pages,
                                m.pruned_by_index,
                                m.pages_scanned,
                                m.cache_hits,
                                m.index_blocks_read,
                                m.index_used
                                    .as_ref()
                                    .map(|ix| format!(" | index='{}'", ix))
                                    .unwrap_or_default()
                            );
                            return Ok((out_cols, found_rows, plan));
                        }
                    }
                }
            }
        }
    }

    // 3. Load base table / CTE / FROM-subquery
    let (mut eval_schema, mut rows): (EvalSchema, Vec<(Option<i64>, Vec<Value>)>) =
        match &sel_eff.from.factor {
            TableFactor::Subquery(sub) => {
                let (scols, srows, _) = execute_select_with_ctes(db, sub, &ctes)?;
                let alias = sel_eff.from.alias.as_deref().unwrap_or("_sub");
                plan_notes.push(format!("SUBQUERY '{}' ({} rows)", alias, srows.len()));
                (
                    EvalSchema::from_names(alias, &scols),
                    srows.into_iter().map(|v| (None, v)).collect(),
                )
            }
            TableFactor::Table(name) => {
                let lower = name.to_ascii_lowercase();
                if let Some((ccols, crows)) = ctes.get(&lower) {
                    let alias = sel_eff.from.alias.as_deref().unwrap_or(name);
                    plan_notes.push(format!("SCAN CTE '{}' ({} rows)", name, crows.len()));
                    (
                        EvalSchema::from_names(alias, ccols),
                        crows.iter().cloned().map(|v| (None, v)).collect(),
                    )
                } else {
                    let base_schema = physical_base.as_ref().unwrap();
                    let alias = sel_eff.from.alias.as_deref().unwrap_or(&base_schema.name);
                    let es = EvalSchema::from_single(alias, base_schema);
                    let scanned = if sel_eff.joins.is_empty() {
                        let mut needed = vec![false; base_schema.columns.len()];
                        let mut all_cols = false;
                        for item in &sel_eff.items {
                            match item {
                                SelectItem::Wildcard(_) => all_cols = true,
                                SelectItem::Expr { expr, .. } => {
                                    collect_expr_columns(expr, base_schema, &mut needed)?;
                                }
                            }
                        }
                        if let Some(ref w) = sel_eff.where_clause {
                            collect_expr_columns(w, base_schema, &mut needed)?;
                        }
                        for g in &sel_eff.group_by {
                            collect_expr_columns(g, base_schema, &mut needed)?;
                        }
                        if let Some(ref h) = sel_eff.having {
                            collect_expr_columns(h, base_schema, &mut needed)?;
                        }
                        for (o, _) in &sel_eff.order_by {
                            collect_expr_columns(o, base_schema, &mut needed)?;
                        }
                        let proj = if all_cols {
                            None
                        } else {
                            Some(needed.as_slice())
                        };
                        if let Some(ref w) = sel_eff.where_clause {
                            validate_where_columns(w, &es)?;
                        }
                        let (sc, m) = db.scan_filtered_with_alias(
                            &base_schema.name,
                            Some(alias),
                            sel_eff.where_clause.as_ref(),
                            proj,
                        )?;
                        let parallel_note = if m.parallel_workers > 1 {
                            format!(" | PARALLEL (workers={})", m.parallel_workers)
                        } else {
                            String::new()
                        };
                        plan_notes.push(format!(
                            "SCAN '{}'{} | total_pages={} | pruned: index={} zonemap={} bloom={} | scanned={} (cache_hits={}, idx_blocks={}){}",
                            base_schema.name,
                            parallel_note,
                            m.total_pages,
                            m.pruned_by_index,
                            m.pruned_by_zonemap,
                            m.pruned_by_bloom,
                            m.pages_scanned,
                            m.cache_hits,
                            m.index_blocks_read,
                            m.index_used
                                .as_ref()
                                .map(|ix| format!(" | index='{}'", ix))
                                .unwrap_or_default()
                        ));
                        sc
                    } else {
                        let (sc, m) =
                            db.scan_filtered_with_metrics(&base_schema.name, None, None)?;
                        plan_notes.push(format!(
                            "SCAN '{}' | total_pages={} | scanned={} (cache_hits={})",
                            base_schema.name, m.total_pages, m.pages_scanned, m.cache_hits
                        ));
                        sc
                    };
                    (
                        es,
                        scanned
                            .into_iter()
                            .map(|r| (Some(r.rowid), r.values))
                            .collect(),
                    )
                }
            }
        };

    // 4. Execute JOINs (Hash Join for equi-joins, nested-loop fallback)
    for join in &sel_eff.joins {
        let (right_alias, right_col_names, right_rows): (String, Vec<String>, Vec<Vec<Value>>) =
            match &join.table.factor {
                TableFactor::Subquery(sub) => {
                    let (scols, srows, _) = execute_select_with_ctes(db, sub, &ctes)?;
                    let a = join.table.alias.clone().unwrap_or_else(|| "_sub".into());
                    (a, scols, srows)
                }
                TableFactor::Table(tname) => {
                    let lower = tname.to_ascii_lowercase();
                    if let Some((ccols, crows)) = ctes.get(&lower) {
                        let a = join.table.alias.clone().unwrap_or_else(|| tname.clone());
                        (a, ccols.clone(), crows.clone())
                    } else {
                        let rs = db
                            .table_schema(tname)
                            .ok_or_else(|| Error::Schema(format!("No such table: '{}'", tname)))?
                            .clone();
                        let a = join.table.alias.clone().unwrap_or_else(|| rs.name.clone());
                        let cnames = rs.columns.iter().map(|c| c.name.clone()).collect();
                        let rrows = db
                            .scan_all(&rs.name)?
                            .into_iter()
                            .map(|r| r.values)
                            .collect();
                        (a, cnames, rrows)
                    }
                }
            };

        let right_width = right_col_names.len();
        let left_width = eval_schema.cols.len();
        if eval_schema.has_alias(&right_alias) {
            return Err(Error::Sql(format!(
                "Duplicate table alias '{}' in FROM/JOIN clause",
                right_alias
            )));
        }
        eval_schema.extend(&right_alias, &right_col_names);

        let equi_cols: Option<(usize, usize)> = if let Expr::Binary {
            left,
            op: BinOp::Eq,
            right,
        } = &join.on
        {
            if let (
                Expr::Column {
                    table: lt,
                    name: ln,
                },
                Expr::Column {
                    table: rt,
                    name: rn,
                },
            ) = (left.as_ref(), right.as_ref())
            {
                let li = eval_schema.resolve(lt.as_deref(), ln).ok();
                let ri = eval_schema.resolve(rt.as_deref(), rn).ok();
                match (li, ri) {
                    (Some(a), Some(b)) if a < left_width && b >= left_width => {
                        Some((a, b - left_width))
                    }
                    (Some(a), Some(b)) if b < left_width && a >= left_width => {
                        Some((b, a - left_width))
                    }
                    _ => None,
                }
            } else {
                None
            }
        } else {
            None
        };

        let mut joined: Vec<(Option<i64>, Vec<Value>)> = Vec::new();
        if let Some((l_idx, r_idx)) = equi_cols {
            plan_notes.push(format!(
                "HASH {:?} JOIN '{}' ({} right rows)",
                join.kind,
                right_alias,
                right_rows.len()
            ));
            let mut ht: HashMap<u64, Vec<usize>> = HashMap::with_capacity(right_rows.len());
            for (ri, rrow) in right_rows.iter().enumerate() {
                if let Some(h) = hash_value(&rrow[r_idx]) {
                    ht.entry(h).or_default().push(ri);
                }
            }
            for (_, lvals) in rows {
                let mut matched_any = false;
                if let Some(h) = hash_value(&lvals[l_idx]) {
                    if let Some(bucket) = ht.get(&h) {
                        for &ri in bucket {
                            if lvals[l_idx].sql_cmp(&right_rows[ri][r_idx]) == Ordering::Equal {
                                let mut combined = Vec::with_capacity(left_width + right_width);
                                combined.extend_from_slice(&lvals);
                                combined.extend_from_slice(&right_rows[ri]);
                                joined.push((None, combined));
                                matched_any = true;
                            }
                        }
                    }
                }
                if !matched_any && join.kind == JoinKind::Left {
                    let mut combined = Vec::with_capacity(left_width + right_width);
                    combined.extend_from_slice(&lvals);
                    combined.resize(left_width + right_width, Value::Null);
                    joined.push((None, combined));
                }
            }
        } else {
            plan_notes.push(format!(
                "NESTED-LOOP {:?} JOIN '{}' ({} right rows)",
                join.kind,
                right_alias,
                right_rows.len()
            ));
            for (_, lvals) in rows {
                let mut matched_any = false;
                for rrow in &right_rows {
                    let mut combined = Vec::with_capacity(left_width + right_width);
                    combined.extend_from_slice(&lvals);
                    combined.extend_from_slice(rrow);
                    if value_to_bool(eval_expr_ctx(
                        &join.on,
                        &eval_schema,
                        Some(&combined),
                        None,
                        None,
                    )?)? == Some(true)
                    {
                        joined.push((None, combined));
                        matched_any = true;
                    }
                }
                if !matched_any && join.kind == JoinKind::Left {
                    let mut combined = Vec::with_capacity(left_width + right_width);
                    combined.extend_from_slice(&lvals);
                    combined.resize(left_width + right_width, Value::Null);
                    joined.push((None, combined));
                }
            }
        }
        rows = joined;
    }

    // 5. Apply WHERE if not already applied during physical single-table scan
    if !sel_eff.joins.is_empty() || physical_base.is_none() {
        if let Some(ref w) = sel_eff.where_clause {
            let mut filtered = Vec::with_capacity(rows.len());
            for (rid, vals) in rows {
                if value_to_bool(eval_expr_ctx(w, &eval_schema, Some(&vals), rid, None)?)?
                    == Some(true)
                {
                    filtered.push((rid, vals));
                }
            }
            rows = filtered;
        }
    }

    let has_agg = !sel_eff.group_by.is_empty()
        || sel_eff.having.is_some()
        || sel_eff.items.iter().any(|it| match it {
            SelectItem::Expr { expr, .. } => expr_contains_aggregate(expr),
            SelectItem::Wildcard(_) => false,
        });

    let mut out_cols = Vec::new();
    for item in &sel_eff.items {
        match item {
            SelectItem::Wildcard(None) => {
                for (_, cn) in &eval_schema.cols {
                    out_cols.push(cn.clone());
                }
            }
            SelectItem::Wildcard(Some(tbl)) => {
                let mut matched_any = false;
                for (ta, cn) in &eval_schema.cols {
                    if ta.eq_ignore_ascii_case(tbl) {
                        out_cols.push(cn.clone());
                        matched_any = true;
                    }
                }
                if !matched_any {
                    return Err(Error::Sql(format!(
                        "Unknown table alias '{}' in '{}.*'",
                        tbl, tbl
                    )));
                }
            }
            SelectItem::Expr { expr, alias } => {
                out_cols.push(alias.clone().unwrap_or_else(|| default_expr_label(expr)));
            }
        }
    }

    let mut out_rows: Vec<(Vec<Value>, Vec<Value>)> = Vec::new();

    if has_agg {
        plan_notes.push(format!("HASH AGGREGATE (group_by={})", sel_eff.group_by.len()));
        let mut group_order: Vec<GroupKey> = Vec::new();
        let mut groups: HashMap<GroupKey, Vec<Vec<Value>>> = HashMap::new();

        if sel_eff.group_by.is_empty() {
            let k = GroupKey(Vec::new());
            group_order.push(k.clone());
            groups.insert(k, rows.into_iter().map(|(_, v)| v).collect());
        } else {
            for (rid, vals) in rows {
                let mut gvals = Vec::with_capacity(sel_eff.group_by.len());
                for gexpr in &sel_eff.group_by {
                    gvals.push(eval_expr_ctx(gexpr, &eval_schema, Some(&vals), rid, None)?);
                }
                let k = GroupKey(gvals);
                if !groups.contains_key(&k) {
                    group_order.push(k.clone());
                }
                groups.entry(k).or_default().push(vals);
            }
        }

        let having_resolved = sel_eff
            .having
            .as_ref()
            .map(|h| substitute_select_aliases(h, &sel_eff.items, &eval_schema));

        for gk in group_order {
            let g_rows = &groups[&gk];
            let rep_row = g_rows.first().map(|v| v.as_slice());
            if let Some(ref h) = having_resolved {
                if value_to_bool(eval_expr_ctx(h, &eval_schema, rep_row, None, Some(g_rows))?)?
                    != Some(true)
                {
                    continue;
                }
            }
            let mut proj = Vec::with_capacity(out_cols.len());
            for item in &sel_eff.items {
                match item {
                    SelectItem::Wildcard(None) => {
                        if let Some(r) = rep_row {
                            proj.extend_from_slice(r);
                        } else {
                            proj.resize(proj.len() + eval_schema.cols.len(), Value::Null);
                        }
                    }
                    SelectItem::Wildcard(Some(tbl)) => {
                        if let Some(r) = rep_row {
                            for (i, (ta, _)) in eval_schema.cols.iter().enumerate() {
                                if ta.eq_ignore_ascii_case(tbl) {
                                    proj.push(r[i].clone());
                                }
                            }
                        } else {
                            for (ta, _) in &eval_schema.cols {
                                if ta.eq_ignore_ascii_case(tbl) {
                                    proj.push(Value::Null);
                                }
                            }
                        }
                    }
                    SelectItem::Expr { expr, .. } => {
                        proj.push(eval_expr_ctx(
                            expr,
                            &eval_schema,
                            rep_row,
                            None,
                            Some(g_rows),
                        )?);
                    }
                }
            }
            let mut skeys = Vec::with_capacity(sel_eff.order_by.len());
            for (oexpr, _) in &sel_eff.order_by {
                if let Expr::Column { table: None, name } = oexpr {
                    if let Some(ai) = out_cols.iter().position(|c| c.eq_ignore_ascii_case(name)) {
                        skeys.push(proj[ai].clone());
                        continue;
                    }
                }
                skeys.push(eval_expr_ctx(
                    oexpr,
                    &eval_schema,
                    rep_row,
                    None,
                    Some(g_rows),
                )?);
            }
            out_rows.push((proj, skeys));
        }
    } else {
        // Pre-compute any Window Function columns across `rows`
        let mut win_cols: HashMap<usize, Vec<Value>> = HashMap::new();
        for (item_idx, item) in sel_eff.items.iter().enumerate() {
            if let SelectItem::Expr {
                expr: Expr::Window { func, over },
                ..
            } = item
            {
                let col_vals = eval_window_column(func, over, &eval_schema, &rows)?;
                win_cols.insert(item_idx, col_vals);
            }
        }
        if !win_cols.is_empty() {
            plan_notes.push(format!("WINDOW ({} funcs)", win_cols.len()));
        }

        for (r_idx, (rid, vals)) in rows.iter().enumerate() {
            let mut proj = Vec::with_capacity(out_cols.len());
            for (item_idx, item) in sel_eff.items.iter().enumerate() {
                match item {
                    SelectItem::Wildcard(None) => proj.extend_from_slice(vals),
                    SelectItem::Wildcard(Some(tbl)) => {
                        for (i, (ta, _)) in eval_schema.cols.iter().enumerate() {
                            if ta.eq_ignore_ascii_case(tbl) {
                                proj.push(vals[i].clone());
                            }
                        }
                    }
                    SelectItem::Expr { expr, .. } => {
                        if let Some(wvals) = win_cols.get(&item_idx) {
                            proj.push(wvals[r_idx].clone());
                        } else {
                            proj.push(eval_expr_ctx(expr, &eval_schema, Some(vals), *rid, None)?);
                        }
                    }
                }
            }
            let mut skeys = Vec::with_capacity(sel_eff.order_by.len());
            for (oexpr, _) in &sel_eff.order_by {
                if let Expr::Column { table: None, name } = oexpr {
                    if let Some(ai) = out_cols.iter().position(|c| c.eq_ignore_ascii_case(name)) {
                        skeys.push(proj[ai].clone());
                        continue;
                    }
                }
                skeys.push(eval_expr_ctx(oexpr, &eval_schema, Some(vals), *rid, None)?);
            }
            out_rows.push((proj, skeys));
        }
    }

    if sel_eff.distinct {
        let mut seen: HashSet<GroupKey> = HashSet::new();
        out_rows.retain(|(proj, _)| seen.insert(GroupKey(proj.clone())));
    }

    if !sel_eff.order_by.is_empty() {
        let dirs: Vec<bool> = sel_eff.order_by.iter().map(|(_, asc)| *asc).collect();
        out_rows.sort_by(|a, b| {
            for (i, &asc) in dirs.iter().enumerate() {
                let o = a.1[i].sql_cmp(&b.1[i]);
                if o != Ordering::Equal {
                    return if asc { o } else { o.reverse() };
                }
            }
            Ordering::Equal
        });
    }

    let start = sel_eff.offset.unwrap_or(0).min(out_rows.len());
    let end = match sel_eff.limit {
        Some(l) => (start + l).min(out_rows.len()),
        None => out_rows.len(),
    };
    let final_rows: Vec<Vec<Value>> = out_rows
        .into_iter()
        .skip(start)
        .take(end - start)
        .map(|(p, _)| p)
        .collect();

    plan_notes.push(format!("OUTPUT -> {} rows", final_rows.len()));
    Ok((out_cols, final_rows, plan_notes.join("\n  -> ")))
}

pub fn execute_statement(db: &mut Database, stmt: Statement) -> Result<QueryResult> {
    match stmt {
        Statement::CreateTable {
            if_not_exists,
            name,
            columns,
            cluster_by,
            check_constraints,
            foreign_keys,
        } => {
            if if_not_exists && db.table_schema(&name).is_some() {
                return Ok(QueryResult::Message(format!(
                    "Table '{}' already exists.",
                    name
                )));
            }
            db.create_table_full(&name, columns, cluster_by, check_constraints, foreign_keys)?;
            Ok(QueryResult::Message(format!("Created table '{}'.", name)))
        }
        Statement::AlterClusterBy { table, cluster_by } => {
            db.set_cluster_by(&table, cluster_by.clone())?;
            Ok(QueryResult::Message(format!(
                "Set CLUSTER BY ({}) on '{}'. Run VACUUM to re-cluster existing pages.",
                cluster_by.join(", "),
                table
            )))
        }
        Statement::AlterAddColumn {
            table,
            column,
            check_constraints,
            foreign_keys,
        } => {
            let cname = column.name.clone();
            db.add_column_with_constraints(&table, column, check_constraints, foreign_keys)?;
            Ok(QueryResult::Message(format!(
                "Added column '{}' to '{}'.",
                cname, table
            )))
        }
        Statement::DropTable { if_exists, name } => {
            if if_exists && db.table_schema(&name).is_none() {
                return Ok(QueryResult::Message(format!(
                    "Table '{}' does not exist.",
                    name
                )));
            }
            db.drop_table(&name)?;
            Ok(QueryResult::Message(format!("Dropped table '{}'.", name)))
        }
        Statement::CreateIndex {
            if_not_exists,
            name,
            table,
            columns,
            desc,
            kind,
            unique,
        } => {
            let cols_display = columns.join(", ");
            db.create_index_full(&name, &table, columns, desc, kind, unique, if_not_exists)?;
            Ok(QueryResult::Message(format!(
                "Created index '{}' on {}({}).",
                name, table, cols_display
            )))
        }
        Statement::DropIndex { if_exists, name } => {
            db.drop_index(&name, if_exists)?;
            Ok(QueryResult::Message(format!("Dropped index '{}'.", name)))
        }
        Statement::Insert {
            table,
            columns,
            rows,
        } => {
            let schema = db
                .table_schema(&table)
                .ok_or_else(|| Error::Schema(format!("No such table: '{}'", table)))?
                .clone();

            let col_indices: Vec<usize> = if let Some(cols) = columns {
                let mut idxs = Vec::with_capacity(cols.len());
                for c in cols {
                    let idx = schema
                        .col_index(&c)
                        .ok_or_else(|| Error::Schema(format!("Unknown column '{}'", c)))?;
                    idxs.push(idx);
                }
                idxs
            } else {
                (0..schema.columns.len()).collect()
            };

            let mut batch = Vec::with_capacity(rows.len());
            for expr_row in rows {
                if expr_row.len() != col_indices.len() {
                    return Err(Error::Sql(format!(
                        "INSERT column count mismatch: expected {}, got {}",
                        col_indices.len(),
                        expr_row.len()
                    )));
                }
                let mut full_vals: Vec<Value> = schema
                    .columns
                    .iter()
                    .map(|c| c.default_value.clone().unwrap_or(Value::Null))
                    .collect();
                for (val_expr, &c_idx) in expr_row.iter().zip(&col_indices) {
                    full_vals[c_idx] = eval_expr(val_expr, &schema, None)?;
                }
                batch.push(full_vals);
            }
            let inserted = db.insert_batch(&table, batch)?;
            Ok(QueryResult::Affected(inserted))
        }
        Statement::Select(sel) => {
            let (columns, rows, _) = execute_select(db, &sel)?;
            Ok(QueryResult::Rows { columns, rows })
        }
        Statement::Explain(sel) => {
            let (_, _, plan) = execute_select(db, &sel)?;
            Ok(QueryResult::Message(format!("QUERY PLAN:\n  -> {}", plan)))
        }
        Statement::Update {
            table,
            assignments,
            where_clause,
        } => {
            let empty_ctes = VirtualTables::new();
            let mut resolved_assigns = Vec::with_capacity(assignments.len());
            for (col, expr) in assignments {
                let re = resolve_subqueries_in_expr(db, &expr, &empty_ctes)?;
                resolved_assigns.push((col, re));
            }
            let resolved_where = match where_clause {
                Some(w) => Some(resolve_subqueries_in_expr(db, &w, &empty_ctes)?),
                None => None,
            };
            let count = db.update_where(&table, &resolved_assigns, resolved_where.as_ref())?;
            Ok(QueryResult::Affected(count))
        }
        Statement::Delete {
            table,
            where_clause,
        } => {
            let empty_ctes = VirtualTables::new();
            let resolved_where = match where_clause {
                Some(w) => Some(resolve_subqueries_in_expr(db, &w, &empty_ctes)?),
                None => None,
            };
            let count = db.delete_where(&table, resolved_where.as_ref())?;
            Ok(QueryResult::Affected(count))
        }
        Statement::Begin => {
            db.begin()?;
            Ok(QueryResult::Message("Transaction started.".into()))
        }
        Statement::Commit => {
            db.commit()?;
            Ok(QueryResult::Message("Transaction committed.".into()))
        }
        Statement::Rollback => {
            db.rollback()?;
            Ok(QueryResult::Message("Transaction rolled back.".into()))
        }
        Statement::Savepoint { name } => {
            db.savepoint(&name)?;
            Ok(QueryResult::Message(format!(
                "Savepoint '{}' created.",
                name
            )))
        }
        Statement::ReleaseSavepoint { name } => {
            db.release_savepoint(&name)?;
            Ok(QueryResult::Message(format!(
                "Savepoint '{}' released.",
                name
            )))
        }
        Statement::RollbackToSavepoint { name } => {
            db.rollback_to_savepoint(&name)?;
            Ok(QueryResult::Message(format!(
                "Rolled back to savepoint '{}'.",
                name
            )))
        }
        Statement::Vacuum { table_sort } => {
            if let Some((tbl, cols)) = table_sort {
                db.set_cluster_by(&tbl, cols)?;
            }
            db.vacuum()?;
            Ok(QueryResult::Message("Database vacuumed.".into()))
        }
        Statement::Checkpoint => {
            db.checkpoint()?;
            Ok(QueryResult::Message("Checkpoint complete.".into()))
        }
        Statement::Backup { path } => {
            db.backup_to(&path)?;
            Ok(QueryResult::Message(format!(
                "Backed up database to '{}'.",
                path
            )))
        }
        Statement::PragmaStats => {
            let stats = db.stats()?;
            Ok(QueryResult::Message(stats.format_report()))
        }
        Statement::PragmaSet { key, value } => {
            let k = key.to_ascii_lowercase();
            match k.as_str() {
                "foreign_keys" => {
                    let on = matches!(
                        value.to_ascii_lowercase().as_str(),
                        "on" | "1" | "true" | "yes"
                    );
                    db.set_foreign_keys_enabled(on);
                    Ok(QueryResult::Message(format!(
                        "foreign_keys = {}",
                        if on { "ON" } else { "OFF" }
                    )))
                }
                "threads" | "max_parallel_workers" | "max_parallel_workers_per_gather" => {
                    let n: usize = value.parse().map_err(|_| {
                        Error::Sql(format!("Invalid integer for {}: '{}'", key, value))
                    })?;
                    db.set_parallel_workers(n);
                    Ok(QueryResult::Message(format!(
                        "parallel_workers = {}",
                        db.parallel_workers()
                    )))
                }
                _ => Err(Error::Sql(format!("Unknown PRAGMA/SET option '{}'", key))),
            }
        }
        Statement::PragmaGet(key) => {
            let k = key.to_ascii_lowercase();
            match k.as_str() {
                "foreign_keys" => Ok(QueryResult::Rows {
                    columns: vec!["foreign_keys".into()],
                    rows: vec![vec![Value::Integer(if db.foreign_keys_enabled() {
                        1
                    } else {
                        0
                    })]],
                }),
                "threads" | "max_parallel_workers" | "max_parallel_workers_per_gather" => {
                    Ok(QueryResult::Rows {
                        columns: vec!["parallel_workers".into()],
                        rows: vec![vec![Value::Integer(db.parallel_workers() as i64)]],
                    })
                }
                "format_version" => Ok(QueryResult::Rows {
                    columns: vec!["format_version".into()],
                    rows: vec![vec![Value::Integer(db.format_version() as i64)]],
                }),
                _ => Err(Error::Sql(format!("Unknown PRAGMA '{}'", key))),
            }
        }
    }
}
