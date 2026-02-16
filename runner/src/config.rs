use std::{env::home_dir, path::PathBuf};

const DEFAULT_DIR: &str = ".sql";

pub struct Config {
    pub(crate) path: PathBuf,
}

impl Config {
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder { path: None }
    }
}

pub struct ConfigBuilder {
    path: Option<PathBuf>,
}

impl ConfigBuilder {
    pub fn path(mut self, path: PathBuf) -> Self {
        self.path = Some(path);
        self
    }

    pub fn build(self) -> Config {
        Config {
            path: self.path.unwrap_or(default_path()),
        }
    }
}

fn default_path() -> PathBuf {
    let mut path = home_dir().expect("cannot get $HOME");
    path.push(DEFAULT_DIR);
    path
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_builder() {
        let config = Config::builder().path(PathBuf::from("test")).build();
        assert!(config.path.to_string_lossy().to_string().ends_with("test"));
    }
}
