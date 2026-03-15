use std::num::NonZeroU32;
use std::sync::Arc;

use serde::de::DeserializeOwned;

use crate::common::{
    DEFAULT_BROWSER_USER_AGENT, GiePage, GieQuery,
    http_core::{
        RateLimitConfig, RateLimiter, RequestContext, build_async_client_with_proxy,
        build_blocking_client_with_proxy, fetch_all_pages, fetch_all_pages_async, fetch_page,
        fetch_page_async,
    },
};
use crate::error::GieError;

pub(crate) trait Endpoint {
    type Record: DeserializeOwned;

    const URL: &'static str;
}

#[derive(Debug, Clone)]
pub(crate) struct BlockingClientCore {
    pub(crate) http: reqwest::blocking::Client,
    pub(crate) api_key: Option<String>,
    pub(crate) user_agent: Option<String>,
    pub(crate) debug_requests: bool,
    pub(crate) rate_limiter: Option<Arc<RateLimiter>>,
}

impl BlockingClientCore {
    pub(crate) fn new(api_key: impl Into<String>) -> Self {
        Self::new_inner(reqwest::blocking::Client::new(), Some(api_key.into()))
    }

    pub(crate) fn without_api_key() -> Self {
        Self::new_inner(reqwest::blocking::Client::new(), None)
    }

    pub(crate) fn with_http_client(
        api_key: Option<String>,
        http: reqwest::blocking::Client,
    ) -> Self {
        Self::new_inner(http, api_key)
    }

    pub(crate) fn with_proxy(
        api_key: Option<String>,
        proxy_url: impl AsRef<str>,
    ) -> Result<Self, GieError> {
        Ok(Self::new_inner(
            build_blocking_client_with_proxy(proxy_url.as_ref())?,
            api_key,
        ))
    }

    fn new_inner(http: reqwest::blocking::Client, api_key: Option<String>) -> Self {
        Self {
            http,
            api_key,
            user_agent: Some(DEFAULT_BROWSER_USER_AGENT.to_string()),
            debug_requests: false,
            rate_limiter: Some(Arc::new(RateLimiter::new(RateLimitConfig::default()))),
        }
    }

    pub(crate) fn with_user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.user_agent = Some(user_agent.into());
        self
    }

    pub(crate) fn without_user_agent(mut self) -> Self {
        self.user_agent = None;
        self
    }

    pub(crate) fn with_debug_requests(mut self, enabled: bool) -> Self {
        self.debug_requests = enabled;
        self
    }

    pub(crate) fn with_rate_limit(mut self, requests_per_minute: NonZeroU32) -> Self {
        self.rate_limiter = Some(Arc::new(RateLimiter::new(RateLimitConfig {
            max_requests_per_minute: requests_per_minute,
            ..RateLimitConfig::default()
        })));
        self
    }

    pub(crate) fn without_rate_limit(mut self) -> Self {
        self.rate_limiter = None;
        self
    }

    pub(crate) fn fetch_page<E>(&self, query: &GieQuery) -> Result<GiePage<E::Record>, GieError>
    where
        E: Endpoint,
    {
        let context = RequestContext {
            api_key: self.api_key.as_deref(),
            user_agent: self.user_agent.as_deref(),
            debug_requests: self.debug_requests,
            rate_limiter: self.rate_limiter.as_deref(),
        };

        fetch_page(&self.http, E::URL, context, query, None)
    }

    pub(crate) fn fetch_all<E>(&self, query: &GieQuery) -> Result<Vec<E::Record>, GieError>
    where
        E: Endpoint,
    {
        let context = RequestContext {
            api_key: self.api_key.as_deref(),
            user_agent: self.user_agent.as_deref(),
            debug_requests: self.debug_requests,
            rate_limiter: self.rate_limiter.as_deref(),
        };

        fetch_all_pages(query.initial_page(), |page| {
            fetch_page(&self.http, E::URL, context, query, Some(page))
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AsyncClientCore {
    pub(crate) http: reqwest::Client,
    pub(crate) api_key: Option<String>,
    pub(crate) user_agent: Option<String>,
    pub(crate) debug_requests: bool,
    pub(crate) rate_limiter: Option<Arc<RateLimiter>>,
}

impl AsyncClientCore {
    pub(crate) fn new(api_key: impl Into<String>) -> Self {
        Self::new_inner(reqwest::Client::new(), Some(api_key.into()))
    }

    pub(crate) fn without_api_key() -> Self {
        Self::new_inner(reqwest::Client::new(), None)
    }

    pub(crate) fn with_http_client(api_key: Option<String>, http: reqwest::Client) -> Self {
        Self::new_inner(http, api_key)
    }

    pub(crate) fn with_proxy(
        api_key: Option<String>,
        proxy_url: impl AsRef<str>,
    ) -> Result<Self, GieError> {
        Ok(Self::new_inner(
            build_async_client_with_proxy(proxy_url.as_ref())?,
            api_key,
        ))
    }

    fn new_inner(http: reqwest::Client, api_key: Option<String>) -> Self {
        Self {
            http,
            api_key,
            user_agent: Some(DEFAULT_BROWSER_USER_AGENT.to_string()),
            debug_requests: false,
            rate_limiter: Some(Arc::new(RateLimiter::new(RateLimitConfig::default()))),
        }
    }

    pub(crate) fn with_user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.user_agent = Some(user_agent.into());
        self
    }

    pub(crate) fn without_user_agent(mut self) -> Self {
        self.user_agent = None;
        self
    }

    pub(crate) fn with_debug_requests(mut self, enabled: bool) -> Self {
        self.debug_requests = enabled;
        self
    }

    pub(crate) fn with_rate_limit(mut self, requests_per_minute: NonZeroU32) -> Self {
        self.rate_limiter = Some(Arc::new(RateLimiter::new(RateLimitConfig {
            max_requests_per_minute: requests_per_minute,
            ..RateLimitConfig::default()
        })));
        self
    }

    pub(crate) fn without_rate_limit(mut self) -> Self {
        self.rate_limiter = None;
        self
    }

    pub(crate) async fn fetch_page<E>(
        &self,
        query: &GieQuery,
    ) -> Result<GiePage<E::Record>, GieError>
    where
        E: Endpoint,
    {
        let context = RequestContext {
            api_key: self.api_key.as_deref(),
            user_agent: self.user_agent.as_deref(),
            debug_requests: self.debug_requests,
            rate_limiter: self.rate_limiter.as_deref(),
        };

        fetch_page_async(&self.http, E::URL, context, query, None).await
    }

    pub(crate) async fn fetch_all<E>(&self, query: &GieQuery) -> Result<Vec<E::Record>, GieError>
    where
        E: Endpoint,
    {
        let context = RequestContext {
            api_key: self.api_key.as_deref(),
            user_agent: self.user_agent.as_deref(),
            debug_requests: self.debug_requests,
            rate_limiter: self.rate_limiter.as_deref(),
        };

        fetch_all_pages_async(query.initial_page(), |page| async move {
            fetch_page_async(&self.http, E::URL, context, query, Some(page)).await
        })
        .await
    }
}

#[cfg(test)]
macro_rules! client_configuration_tests {
    ($blocking_client:ident, $async_client:ident) => {
        #[test]
        fn clients_can_be_created_without_api_key() {
            let blocking_client = $blocking_client::without_api_key();
            let async_client = $async_client::without_api_key();

            assert!(blocking_client.core.api_key.is_none());
            assert!(async_client.core.api_key.is_none());
            assert_eq!(
                blocking_client.core.user_agent.as_deref(),
                Some(crate::common::DEFAULT_BROWSER_USER_AGENT)
            );
            assert_eq!(
                async_client.core.user_agent.as_deref(),
                Some(crate::common::DEFAULT_BROWSER_USER_AGENT)
            );
        }

        #[test]
        fn clients_can_be_created_with_proxy() {
            let blocking_client =
                $blocking_client::with_proxy("key", "http://127.0.0.1:8080").unwrap();
            let async_client = $async_client::with_proxy("key", "http://127.0.0.1:8080").unwrap();

            assert!(blocking_client.core.api_key.is_some());
            assert!(async_client.core.api_key.is_some());
        }

        #[test]
        fn debug_flag_can_be_enabled() {
            let blocking_client = $blocking_client::without_api_key().with_debug_requests(true);
            let async_client = $async_client::without_api_key().with_debug_requests(true);

            assert!(blocking_client.core.debug_requests);
            assert!(async_client.core.debug_requests);
        }

        #[test]
        fn rate_limit_can_be_configured_or_disabled() {
            let blocking_default = $blocking_client::without_api_key();
            let async_default = $async_client::without_api_key();
            assert!(blocking_default.core.rate_limiter.is_some());
            assert!(async_default.core.rate_limiter.is_some());

            let blocking_custom = $blocking_client::without_api_key()
                .with_rate_limit(std::num::NonZeroU32::new(30).unwrap())
                .without_rate_limit();
            let async_custom = $async_client::without_api_key()
                .with_rate_limit(std::num::NonZeroU32::new(30).unwrap())
                .without_rate_limit();

            assert!(blocking_custom.core.rate_limiter.is_none());
            assert!(async_custom.core.rate_limiter.is_none());
        }

        #[test]
        fn user_agent_can_be_overridden_and_disabled() {
            let blocking_client = $blocking_client::without_api_key()
                .with_user_agent("custom-agent/1.0")
                .without_user_agent();
            let async_client = $async_client::without_api_key()
                .with_user_agent("custom-agent/1.0")
                .without_user_agent();

            assert!(blocking_client.core.user_agent.is_none());
            assert!(async_client.core.user_agent.is_none());
        }
    };
}

#[cfg(test)]
pub(crate) use client_configuration_tests;
