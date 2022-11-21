use std::fmt::Display;

use actix_web::{body::BoxBody, http::StatusCode, HttpResponse, ResponseError};

#[derive(Debug)]
pub struct Error {
    msg: String,
    code: StatusCode,
}

impl Error {
    pub fn new(msg: String, code: StatusCode) -> Self {
        Self { msg, code }
    }

    pub fn new_404(msg: String) -> Self {
        Self { msg, code: StatusCode::NOT_FOUND }
    }

    pub fn new_500(msg: String) -> Self {
        Self {
            msg,
            code: StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl ResponseError for Error {
    fn error_response(&self) -> HttpResponse<BoxBody> {
        HttpResponse::with_body(self.code, BoxBody::new(self.msg.clone()))
    }
    fn status_code(&self) -> StatusCode {
        self.code
    }
}

impl<T: std::error::Error> From<T> for Error {
    fn from(value: T) -> Self {
        Self {
            msg: format!("{}", value),
            code: StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}
