using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Messages;
using DotNetCore.CAP.Persistence;
using DotNetCore.CAP.Processor;
using DotNetCore.CAP.Test.Helpers;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Xunit;

namespace DotNetCore.CAP.Test;

public class DispatcherTests
{
    private readonly ILogger<Dispatcher> _logger;
    private readonly ISubscribeExecutor _executor;
    private readonly IDataStorage _storage;

    public DispatcherTests()
    {
        _logger = Substitute.For<ILogger<Dispatcher>>();
        _executor = Substitute.For<ISubscribeExecutor>();
        _storage = Substitute.For<IDataStorage>();
    }

    [Fact]
    public async Task EnqueueToPublish_ShouldInvokeSend_WhenParallelSendDisabled()
    {
        // Arrange
        var sender = new TestThreadSafeMessageSender();
        var options = Options.Create(new CapOptions
        {
            EnableSubscriberParallelExecute = true,
            EnablePublishParallelSend = false,
            SubscriberParallelExecuteThreadCount = 2,
            SubscriberParallelExecuteBufferFactor = 2
        });

        var dispatcher = new Dispatcher(_logger, sender, options, _executor, _storage);

        using var cts = new CancellationTokenSource();
        var messageId = "testId";

        // Act
        await dispatcher.StartAsync(cts.Token);
        await dispatcher.EnqueueToPublish(CreateTestMessage(messageId));
        await cts.CancelAsync();

        // Assert
        sender.Count.Should().Be(1);
        sender.ReceivedMessages.First().DbId.Should().Be(messageId);
    }

    [Fact]
    public async Task EnqueueToPublish_ShouldBeThreadSafe_WhenParallelSendDisabled()
    {
        // Arrange
        var sender = new TestThreadSafeMessageSender();
        var options = Options.Create(new CapOptions
        {
            EnableSubscriberParallelExecute = true,
            EnablePublishParallelSend = false,
            SubscriberParallelExecuteThreadCount = 2,
            SubscriberParallelExecuteBufferFactor = 2
        });
        var dispatcher = new Dispatcher(_logger, sender, options, _executor, _storage);

        using var cts = new CancellationTokenSource();
        var messages = Enumerable.Range(1, 100)
            .Select(i => CreateTestMessage(i.ToString()))
            .ToArray();

        // Act
        await dispatcher.StartAsync(cts.Token);

        var tasks = messages
            .Select(msg => Task.Run(() => dispatcher.EnqueueToPublish(msg), CancellationToken.None));
        await Task.WhenAll(tasks);
        await cts.CancelAsync();

        // Assert
        sender.Count.Should().Be(100);
        var receivedMessages = sender.ReceivedMessages.Select(m => m.DbId).Order().ToList();
        var expected = messages.Select(m => m.DbId).Order().ToList();
        expected.Should().Equal(receivedMessages);
    }

    [Fact]
    public async Task EnqueueToScheduler_ShouldBeThreadSafe_WhenDelayLessThenMinute()
    {
        // Arrange
        var sender = new TestThreadSafeMessageSender();
        var options = Options.Create(new CapOptions
        {
            EnableSubscriberParallelExecute = true,
            EnablePublishParallelSend = false,
            SubscriberParallelExecuteThreadCount = 2,
            SubscriberParallelExecuteBufferFactor = 2
        });
        var dispatcher = new Dispatcher(_logger, sender, options, _executor, _storage);

        using var cts = new CancellationTokenSource();
        var messages = Enumerable.Range(1, 10000)
            .Select(i => CreateTestMessage(i.ToString()))
            .ToArray();

        // Act
        await dispatcher.StartAsync(cts.Token);
        var dateTime = DateTime.Now.AddSeconds(1);
        await Parallel.ForEachAsync(messages, CancellationToken.None,
            async (m, ct) => { await dispatcher.EnqueueToScheduler(m, dateTime); });

        await Task.Delay(1500, CancellationToken.None);

        await cts.CancelAsync();

        // Assert
        sender.Count.Should().Be(10000);

        var receivedMessages = sender.ReceivedMessages.Select(m => m.DbId).Order().ToList();
        var expected = messages.Select(m => m.DbId).Order().ToList();
        expected.Should().Equal(receivedMessages);
    }

    [Fact]
    public async Task EnqueueToScheduler_ShouldSendMessagesInCorrectOrder_WhenEarlierMessageIsSentLater()
    {
        // Arrange
        var sender = new TestThreadSafeMessageSender();
        var options = Options.Create(new CapOptions
        {
            EnableSubscriberParallelExecute = true,
            EnablePublishParallelSend = false,
            SubscriberParallelExecuteThreadCount = 2,
            SubscriberParallelExecuteBufferFactor = 2
        });
        var dispatcher = new Dispatcher(_logger, sender, options, _executor, _storage);

        using var cts = new CancellationTokenSource();
        var messages = Enumerable.Range(1, 3)
            .Select(i => CreateTestMessage(i.ToString()))
            .ToArray();

        // Act
        await dispatcher.StartAsync(cts.Token);
        var dateTime = DateTime.Now;

        await dispatcher.EnqueueToScheduler(messages[0], dateTime.AddSeconds(1));
        await dispatcher.EnqueueToScheduler(messages[1], dateTime.AddMilliseconds(200));
        await dispatcher.EnqueueToScheduler(messages[2], dateTime.AddMilliseconds(100));

        await Task.Delay(1200, CancellationToken.None);
        await cts.CancelAsync();

        // Assert
        sender.ReceivedMessages.Select(m => m.DbId).Should().Equal(["3", "2", "1"]);
    }

    [Fact]
    public async Task EnqueueToScheduler_ShouldBeThreadSafe_WhenDelayLessThenMinuteAndParallelSendEnabled()
    {
        // Arrange
        var sender = new TestThreadSafeMessageSender();
        var options = Options.Create(new CapOptions
        {
            EnableSubscriberParallelExecute = false,
            EnablePublishParallelSend = true,
            SubscriberParallelExecuteThreadCount = 2,
            SubscriberParallelExecuteBufferFactor = 2
        });
        var dispatcher = new Dispatcher(_logger, sender, options, _executor, _storage);

        using var cts = new CancellationTokenSource();
        var messages = Enumerable.Range(1, 10000)
            .Select(i => CreateTestMessage(i.ToString()))
            .ToArray();

        // Act
        await dispatcher.StartAsync(cts.Token);
        var dateTime = DateTime.Now.AddMilliseconds(50);
        await Parallel.ForEachAsync(messages, CancellationToken.None,
            async (m, ct) => { await dispatcher.EnqueueToScheduler(m, dateTime); });

        await Task.Delay(3000, CancellationToken.None);

        await cts.CancelAsync();

        // Assert
        sender.Count.Should().Be(10000);

        var receivedMessages = sender.ReceivedMessages.Select(m => m.DbId).Order().ToList();
        var expected = messages.Select(m => m.DbId).Order().ToList();
        expected.Should().Equal(receivedMessages);
    }

    [Fact]
    public async Task EnqueueToScheduler_ShouldSendMessagesInCorrectOrder_WhenParallelSendEnabled()
    {
        // Arrange
        var sender = new TestThreadSafeMessageSender();
        var options = Options.Create(new CapOptions
        {
            EnableSubscriberParallelExecute = true,
            EnablePublishParallelSend = true,
            SubscriberParallelExecuteThreadCount = 2,
            SubscriberParallelExecuteBufferFactor = 2,
        });
        var dispatcher = new Dispatcher(_logger, sender, options, _executor, _storage);

        using var cts = new CancellationTokenSource();
        var messages = Enumerable.Range(1, 3)
            .Select(i => CreateTestMessage(i.ToString()))
            .ToArray();

        // Act
        await dispatcher.StartAsync(cts.Token);
        var dateTime = DateTime.Now;

        await dispatcher.EnqueueToScheduler(messages[0], dateTime.AddSeconds(1));
        await dispatcher.EnqueueToScheduler(messages[1], dateTime.AddMilliseconds(200));
        await dispatcher.EnqueueToScheduler(messages[2], dateTime.AddMilliseconds(100));

        await Task.Delay(1200, CancellationToken.None);
        await cts.CancelAsync();

        // Assert
        sender.ReceivedMessages.Select(m => m.DbId).Should().Equal(["3", "2", "1"]);
    }

    [Fact]
    public async Task EnqueueToExecute_ReturnsFalse_WhenCancellationRequested()
    {
        // Arrange
        var options = Options.Create(new CapOptions { EnableSubscriberParallelExecute = false });
        var dispatcher = new Dispatcher(_logger, new TestThreadSafeMessageSender(), options, _executor, _storage);
        using var cts = new CancellationTokenSource();
        await dispatcher.StartAsync(cts.Token);

        // Act
        await cts.CancelAsync();
        var accepted = await dispatcher.EnqueueToExecute(CreateTestMessage());

        // Assert
        accepted.Should().BeFalse();
        await _executor.DidNotReceive()
            .ExecuteAsync(Arg.Any<MediumMessage>(), Arg.Any<ConsumerExecutorDescriptor>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task EnqueueToExecute_ReturnsTrue_AndExecutesInline_InSyncMode()
    {
        // Arrange
        _executor.ExecuteAsync(Arg.Any<MediumMessage>(), Arg.Any<ConsumerExecutorDescriptor>(), Arg.Any<CancellationToken>())
            .Returns(OperateResult.Success);
        var options = Options.Create(new CapOptions { EnableSubscriberParallelExecute = false });
        var dispatcher = new Dispatcher(_logger, new TestThreadSafeMessageSender(), options, _executor, _storage);
        using var cts = new CancellationTokenSource();
        await dispatcher.StartAsync(cts.Token);
        var message = CreateTestMessage();

        // Act
        var accepted = await dispatcher.EnqueueToExecute(message);

        // Assert
        accepted.Should().BeTrue();
        await _executor.Received(1)
            .ExecuteAsync(message, Arg.Any<ConsumerExecutorDescriptor>(), Arg.Any<CancellationToken>());
        await cts.CancelAsync();
    }

    [Fact]
    public async Task EnqueueToExecute_ReturnsFalse_WhenChannelCompletedByDrain_InParallelMode()
    {
        // Arrange
        var options = Options.Create(new CapOptions
        {
            EnableSubscriberParallelExecute = true,
            SubscriberParallelExecuteThreadCount = 1,
            SubscriberParallelExecuteBufferFactor = 10
        });
        var dispatcher = new Dispatcher(_logger, new TestThreadSafeMessageSender(), options, _executor, _storage);
        using var cts = new CancellationTokenSource();
        await dispatcher.StartAsync(cts.Token);

        // Act - drain completes the received channel writer.
        await dispatcher.DrainReceivedAsync(TimeSpan.FromSeconds(2));
        var accepted = await dispatcher.EnqueueToExecute(CreateTestMessage());

        // Assert
        accepted.Should().BeFalse();
        await cts.CancelAsync();
    }

    [Fact]
    public async Task DrainReceivedAsync_ExecutesBufferedMessages_WithinGrace()
    {
        // Arrange
        _executor.ExecuteAsync(Arg.Any<MediumMessage>(), Arg.Any<ConsumerExecutorDescriptor>(), Arg.Any<CancellationToken>())
            .Returns(OperateResult.Success);
        var options = Options.Create(new CapOptions
        {
            EnableSubscriberParallelExecute = true,
            EnableImmediateRetryOnShutdown = true,
            SubscriberParallelExecuteThreadCount = 1,
            SubscriberParallelExecuteBufferFactor = 100
        });
        var dispatcher = new Dispatcher(_logger, new TestThreadSafeMessageSender(), options, _executor, _storage);
        using var cts = new CancellationTokenSource();
        await dispatcher.StartAsync(cts.Token);

        for (var i = 1; i <= 5; i++)
        {
            (await dispatcher.EnqueueToExecute(CreateTestMessage(i.ToString()))).Should().BeTrue();
        }

        // Act
        await dispatcher.DrainReceivedAsync(TimeSpan.FromSeconds(5));

        // Assert - all buffered messages executed, nothing flagged for immediate retry.
        await _executor.Received(5)
            .ExecuteAsync(Arg.Any<MediumMessage>(), Arg.Any<ConsumerExecutorDescriptor>(), Arg.Any<CancellationToken>());
        await _storage.DidNotReceive().ChangeReceiveStateToImmediateRetryAsync(Arg.Any<string[]>());
        await cts.CancelAsync();
    }

    [Fact]
    public async Task DrainReceivedAsync_FlagsRemainingForImmediateRetry_OnTimeout()
    {
        // Arrange - the executor blocks on the first message so the rest stay buffered.
        var firstCallStarted = new TaskCompletionSource();
        var blocker = new TaskCompletionSource<OperateResult>();
        _executor.ExecuteAsync(Arg.Any<MediumMessage>(), Arg.Any<ConsumerExecutorDescriptor>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                firstCallStarted.TrySetResult();
                return blocker.Task;
            });

        var options = Options.Create(new CapOptions
        {
            EnableSubscriberParallelExecute = true,
            EnableImmediateRetryOnShutdown = true,
            SubscriberParallelExecuteThreadCount = 1,
            SubscriberParallelExecuteBufferFactor = 100
        });
        var dispatcher = new Dispatcher(_logger, new TestThreadSafeMessageSender(), options, _executor, _storage);
        using var cts = new CancellationTokenSource();
        await dispatcher.StartAsync(cts.Token);

        for (var i = 1; i <= 5; i++)
        {
            (await dispatcher.EnqueueToExecute(CreateTestMessage(i.ToString()))).Should().BeTrue();
        }

        // Ensure exactly one message is picked up (and stuck) before draining; the other 4 remain buffered.
        await firstCallStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Act - drain cannot finish because the executor is blocked.
        await dispatcher.DrainReceivedAsync(TimeSpan.FromMilliseconds(300));

        // Assert - the 4 still-buffered messages are flagged for immediate retry.
        await _storage.Received(1)
            .ChangeReceiveStateToImmediateRetryAsync(Arg.Is<string[]>(ids => ids.Length == 4));

        blocker.TrySetResult(OperateResult.Success);
        await cts.CancelAsync();
    }

    [Fact]
    public async Task DrainReceivedAsync_DoesNotFlag_WhenImmediateRetryDisabled()
    {
        // Arrange
        var firstCallStarted = new TaskCompletionSource();
        var blocker = new TaskCompletionSource<OperateResult>();
        _executor.ExecuteAsync(Arg.Any<MediumMessage>(), Arg.Any<ConsumerExecutorDescriptor>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                firstCallStarted.TrySetResult();
                return blocker.Task;
            });

        var options = Options.Create(new CapOptions
        {
            EnableSubscriberParallelExecute = true,
            EnableImmediateRetryOnShutdown = false,
            SubscriberParallelExecuteThreadCount = 1,
            SubscriberParallelExecuteBufferFactor = 100
        });
        var dispatcher = new Dispatcher(_logger, new TestThreadSafeMessageSender(), options, _executor, _storage);
        using var cts = new CancellationTokenSource();
        await dispatcher.StartAsync(cts.Token);

        for (var i = 1; i <= 5; i++)
        {
            await dispatcher.EnqueueToExecute(CreateTestMessage(i.ToString()));
        }

        await firstCallStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Act
        await dispatcher.DrainReceivedAsync(TimeSpan.FromMilliseconds(300));

        // Assert
        await _storage.DidNotReceive().ChangeReceiveStateToImmediateRetryAsync(Arg.Any<string[]>());

        blocker.TrySetResult(OperateResult.Success);
        await cts.CancelAsync();
    }

    [Fact]
    public async Task DrainReceivedAsync_IsNoOp_InSyncMode()
    {
        // Arrange
        var options = Options.Create(new CapOptions { EnableSubscriberParallelExecute = false });
        var dispatcher = new Dispatcher(_logger, new TestThreadSafeMessageSender(), options, _executor, _storage);
        using var cts = new CancellationTokenSource();
        await dispatcher.StartAsync(cts.Token);

        // Act
        await dispatcher.DrainReceivedAsync(TimeSpan.FromSeconds(1));

        // Assert - nothing to drain, nothing flagged.
        await _storage.DidNotReceive().ChangeReceiveStateToImmediateRetryAsync(Arg.Any<string[]>());
        await cts.CancelAsync();
    }

    private MediumMessage CreateTestMessage(string id = "1")
    {
        return new MediumMessage()
        {
            DbId = id,
            Origin = new Message(
                headers: new Dictionary<string, string>()
                {
                    { "cap-msg-id", id }
                },
                value: new MessageValue("test@test.com", "User"))
        };
    }
}