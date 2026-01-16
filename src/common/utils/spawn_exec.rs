#[derive(Clone)]
pub struct SpawnExec;
impl<Fut> hyper::rt::Executor<Fut> for SpawnExec
where
    Fut: std::future::Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    fn execute(&self, fut: Fut) {
        tokio::spawn(fut);
    }
}
