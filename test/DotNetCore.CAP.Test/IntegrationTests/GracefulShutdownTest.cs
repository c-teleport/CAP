using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DotNetCore.CAP.Test.Helpers;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;
using Xunit.Abstractions;

namespace DotNetCore.CAP.Test.IntegrationTests
{
    public class GracefulShutdownTest : IntegrationTestBase
    {
        public GracefulShutdownTest(ITestOutputHelper testOutput)
            : base(testOutput)
        {
        }

        [Fact]
        public async Task GracefulShutdown_WaitsForInflightHandler_ToComplete()
        {
            var gate = Container.GetRequiredService<ShutdownGate>();

            // Publish on a background thread; in synchronous mode the handler blocks inside the consumer callback.
            _ = Task.Run(() => Publisher.PublishAsync(nameof(GracefulShutdownTest), "Test Message"), CancellationToken);

            // Wait until the handler is actually executing (i.e. it is an in-flight consumed message).
            await gate.Started.Task.WaitAsync(CancellationToken);

            // Begin graceful shutdown. It must NOT return while the handler is still running.
            var bootstrapper = (IHostedService)Container.GetRequiredService<IBootstrapper>();
            var stopTask = bootstrapper.StopAsync(CancellationToken.None);

            await Task.Delay(300, CancellationToken);
            stopTask.IsCompleted.Should()
                .BeFalse("graceful shutdown must drain the in-flight handler before returning");

            // Release the handler; graceful shutdown should now complete and the message should be handled.
            gate.Release.SetResult();
            await stopTask.WaitAsync(CancellationToken);

            HandledMessages.OfType<string>().Should().Contain("completed");
        }

        protected override void ConfigureServices(IServiceCollection services)
        {
            services.AddSingleton<ShutdownGate>();
            services.AddTransient<BlockingSubscriber>();
        }

        public class ShutdownGate
        {
            public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

            public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        private class BlockingSubscriber : ICapSubscribe
        {
            private readonly ShutdownGate _gate;
            private readonly TestMessageCollector _collector;

            public BlockingSubscriber(ShutdownGate gate, TestMessageCollector collector)
            {
                _gate = gate;
                _collector = collector;
            }

            [CapSubscribe(nameof(GracefulShutdownTest), Group = TestServiceCollectionExtensions.TestGroupName)]
            public async Task Handle(string message)
            {
                _gate.Started.TrySetResult();
                await _gate.Release.Task;
                _collector.Add("completed");
            }
        }
    }
}
