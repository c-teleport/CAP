using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Messages;
using DotNetCore.CAP.Persistence;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace DotNetCore.CAP.Test;

public class ReceivedMessageLeaseStorageTests
{
    private static readonly TimeSpan Lookback = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(30);

    // Note: InMemoryStorage uses static dictionaries shared across parallel tests, so these tests only assert
    // about their own (uniquely-generated) message ids rather than clearing the shared store.
    private static IDataStorage CreateStorage(int leaseSeconds = 60)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddCap(x =>
        {
            x.ReceivedMessageLeaseSeconds = leaseSeconds;
            x.UseInMemoryStorage();
        });
        return services.BuildServiceProvider().GetRequiredService<IDataStorage>();
    }

    [Fact]
    public async Task GetReceivedMessagesOfNeedRetry_DoesNotReturn_RecentScheduledMessage()
    {
        var storage = CreateStorage();
        var message = await storage.StoreReceivedMessageAsync("test.topic", "group", CreateMessage());

        (await storage.GetReceivedMessagesOfNeedRetry(Lookback)).Select(m => m.DbId).Should().NotContain(message.DbId);
    }

    [Fact]
    public async Task StoreReceivedMessage_LeasesTheRowToTheStoringInstance()
    {
        var storage = CreateStorage(leaseSeconds: 60);

        var message = await storage.StoreReceivedMessageAsync("test.topic", "group", CreateMessage());

        message.StatusName.Should().Be(StatusName.Scheduled);
        message.LeasedUntil.Should().BeCloseTo(DateTime.Now.AddSeconds(60), TimeSpan.FromSeconds(5));
        // Even with no lookback at all the row stays hidden: the lease, not Added, decides eligibility now.
        (await storage.GetReceivedMessagesOfNeedRetry(TimeSpan.Zero)).Select(m => m.DbId).Should().NotContain(message.DbId);
    }

    [Fact]
    public async Task GetReceivedMessagesOfNeedRetry_ReturnsFreshRow_OnceItsInitialLeaseExpired()
    {
        var storage = CreateStorage(leaseSeconds: -1);

        var message = await storage.StoreReceivedMessageAsync("test.topic", "group", CreateMessage());

        // The row was added a moment ago, well inside the lookback; the expired lease alone makes it eligible.
        var recovered = (await storage.GetReceivedMessagesOfNeedRetry(Lookback)).Single(m => m.DbId == message.DbId);
        recovered.StatusName.Should().Be(StatusName.Scheduled);
        (await storage.TryLeaseReceivedMessageAsync(recovered, Lease)).Should().BeTrue();
    }

    [Fact]
    public async Task GetReceivedMessagesOfNeedRetry_SkipsRow_StoredWithinTheLeasePeriod()
    {
        var storage = CreateStorage(leaseSeconds: 60);
        var picked = await StoreFlaggedAndReadAsync(storage);

        // A lease shorter than the configured one: recovery waits until the row is one configured lease old.
        (await storage.TryLeaseReceivedMessageAsync(picked, TimeSpan.FromSeconds(-1))).Should().BeTrue();

        (await storage.GetReceivedMessagesOfNeedRetry(Lookback)).Select(m => m.DbId).Should().NotContain(picked.DbId);
    }

    [Fact]
    public async Task TryRenew_ExtendsTheLease_WhileTheCallerStillHoldsIt()
    {
        var storage = CreateStorage(leaseSeconds: 1);
        var message = await storage.StoreReceivedMessageAsync("test.topic", "group", CreateMessage());
        var before = message.LeasedUntil!.Value;

        (await storage.TryRenewReceivedMessageLeaseAsync(message, Lease)).Should().BeTrue();

        message.LeasedUntil.Should().BeAfter(before);
        message.LeasedUntil.Should().BeCloseTo(DateTime.Now.Add(Lease), TimeSpan.FromSeconds(5));
        (await storage.GetReceivedMessagesOfNeedRetry(TimeSpan.Zero)).Select(m => m.DbId).Should().NotContain(message.DbId);
    }

    [Fact]
    public async Task TryRenew_RefusesACaller_WhoseLeaseWasTakenOver()
    {
        var storage = CreateStorage(leaseSeconds: -1);
        var lost = await storage.StoreReceivedMessageAsync("test.topic", "group", CreateMessage());
        var taken = (await storage.GetReceivedMessagesOfNeedRetry(Lookback)).Single(m => m.DbId == lost.DbId);
        (await storage.TryLeaseReceivedMessageAsync(taken, Lease)).Should().BeTrue();

        (await storage.TryRenewReceivedMessageLeaseAsync(lost, Lease)).Should().BeFalse();
        (await storage.TryRenewReceivedMessageLeaseAsync(taken, Lease)).Should().BeTrue();
    }

    [Fact]
    public async Task ChangeReceiveState_IsFencedOut_WhenTheLeaseWasTakenOver()
    {
        var storage = CreateStorage(leaseSeconds: -1);
        var lost = await storage.StoreReceivedMessageAsync("test.topic", "group", CreateMessage());
        var taken = (await storage.GetReceivedMessagesOfNeedRetry(Lookback)).Single(m => m.DbId == lost.DbId);
        (await storage.TryLeaseReceivedMessageAsync(taken, Lease)).Should().BeTrue();

        // The late run finishes with Failed after the row was taken over; its write must not land.
        lost.Retries = 1;
        lost.ExpiresAt = DateTime.Now.AddHours(1);
        lost.LeasedUntil = null;
        (await storage.ChangeReceiveStateAsync(lost, StatusName.Failed)).Should().BeFalse();

        (await storage.GetReceivedMessagesOfNeedRetry(TimeSpan.Zero)).Select(m => m.DbId).Should().NotContain(lost.DbId);
        taken.ExpiresAt = DateTime.Now.AddHours(1);
        taken.LeasedUntil = null;
        (await storage.ChangeReceiveStateAsync(taken, StatusName.Succeeded)).Should().BeTrue();
    }

    [Fact]
    public async Task ChangeReceiveState_Applies_ForTheOwner_AfterItsRowWasFlaggedOnShutdown()
    {
        var storage = CreateStorage();
        var message = await storage.StoreReceivedMessageAsync("test.topic", "group", CreateMessage());
        await storage.ChangeReceiveStateToImmediateRetryAsync(new[] { message.DbId });

        message.ExpiresAt = DateTime.Now.AddHours(1);
        message.LeasedUntil = null;
        (await storage.ChangeReceiveStateAsync(message, StatusName.Succeeded)).Should().BeTrue();

        (await storage.GetReceivedMessagesOfNeedRetry(TimeSpan.Zero)).Select(m => m.DbId).Should().NotContain(message.DbId);
    }

    [Fact]
    public async Task ChangeReceiveState_IsFencedOut_WhenAFlaggedRowWasAlreadyTakenByAnotherRun()
    {
        var storage = CreateStorage();
        var message = await storage.StoreReceivedMessageAsync("test.topic", "group", CreateMessage());
        await storage.ChangeReceiveStateToImmediateRetryAsync(new[] { message.DbId });
        var taken = (await storage.GetReceivedMessagesOfNeedRetry(Lookback)).Single(m => m.DbId == message.DbId);
        (await storage.TryLeaseReceivedMessageAsync(taken, Lease)).Should().BeTrue();

        message.ExpiresAt = DateTime.Now.AddHours(1);
        message.LeasedUntil = null;
        (await storage.ChangeReceiveStateAsync(message, StatusName.Succeeded)).Should().BeFalse();
    }

    [Fact]
    public async Task ChangeReceiveState_Applies_WhenTheRunHoldsNoLease()
    {
        var storage = CreateStorage();
        var stored = await storage.StoreReceivedMessageAsync("test.topic", "group", CreateMessage());

        // A dashboard re-execute or a pre-upgrade row: no lease to fence on, the write goes through.
        var unleased = new MediumMessage { DbId = stored.DbId, Origin = stored.Origin, ExpiresAt = DateTime.Now.AddHours(1) };
        (await storage.ChangeReceiveStateAsync(unleased, StatusName.Succeeded)).Should().BeTrue();

        (await storage.ChangeReceiveStateAsync(stored, StatusName.Failed)).Should().BeFalse();
    }

    [Fact]
    public async Task TryRenew_RefusesAMessage_WithoutALease()
    {
        var storage = CreateStorage();
        var message = await storage.StoreReceivedMessageAsync("test.topic", "group", CreateMessage());
        message.LeasedUntil = null;

        (await storage.TryRenewReceivedMessageLeaseAsync(message, Lease)).Should().BeFalse();
    }

    [Fact]
    public async Task ChangeReceiveStateToImmediateRetry_MakesRecentMessageEligibleImmediately()
    {
        var storage = CreateStorage();
        var message = await storage.StoreReceivedMessageAsync("test.topic", "group", CreateMessage());

        await storage.ChangeReceiveStateToImmediateRetryAsync(new[] { message.DbId });

        var picked = (await storage.GetReceivedMessagesOfNeedRetry(Lookback)).Single(m => m.DbId == message.DbId);
        picked.StatusName.Should().Be(StatusName.RetryImmediately);
        picked.LeasedUntil.Should().BeNull();
    }

    [Fact]
    public async Task TryLease_GrantsOnce_AndHidesTheMessageWhileTheLeaseIsValid()
    {
        var storage = CreateStorage();
        var picked = await StoreFlaggedAndReadAsync(storage);

        (await storage.TryLeaseReceivedMessageAsync(picked, Lease)).Should().BeTrue();

        picked.StatusName.Should().Be(StatusName.Scheduled);
        picked.LeasedUntil.Should().BeCloseTo(DateTime.Now.Add(Lease), TimeSpan.FromSeconds(5));
        (await storage.TryLeaseReceivedMessageAsync(picked, Lease)).Should().BeFalse();
        (await storage.GetReceivedMessagesOfNeedRetry(Lookback)).Select(m => m.DbId).Should().NotContain(picked.DbId);
    }

    [Fact]
    public async Task GetReceivedMessagesOfNeedRetry_ReturnsLeasedMessage_OnceTheLeaseExpired()
    {
        var storage = CreateStorage(leaseSeconds: -1);
        var picked = await StoreFlaggedAndReadAsync(storage);

        // A lease that is already over models a retry run that died without writing a final state.
        (await storage.TryLeaseReceivedMessageAsync(picked, TimeSpan.FromSeconds(-1))).Should().BeTrue();

        var recovered = (await storage.GetReceivedMessagesOfNeedRetry(Lookback)).Single(m => m.DbId == picked.DbId);
        recovered.StatusName.Should().Be(StatusName.Scheduled);
        (await storage.TryLeaseReceivedMessageAsync(recovered, Lease)).Should().BeTrue();
    }

    [Fact]
    public async Task TryLease_RefusesMessage_WhoseStatusChangedSinceItWasRead()
    {
        var storage = CreateStorage();
        var picked = await StoreFlaggedAndReadAsync(storage);

        // The draining instance finishes the original run after the row was selected for retry.
        var finished = new MediumMessage { DbId = picked.DbId, Origin = picked.Origin, ExpiresAt = DateTime.Now.AddHours(1) };
        await storage.ChangeReceiveStateAsync(finished, StatusName.Succeeded);

        (await storage.TryLeaseReceivedMessageAsync(picked, Lease)).Should().BeFalse();
    }

    [Fact]
    public async Task TryLease_RefusesMessage_WhoseRetriesChangedSinceItWasRead()
    {
        var storage = CreateStorage();
        var picked = await StoreFlaggedAndReadAsync(storage);

        // Another retry run of the same row failed in between: same status as before, one more attempt recorded.
        await storage.ChangeReceiveStateToImmediateRetryAsync(new[] { picked.DbId });
        var afterFailure = new MediumMessage { DbId = picked.DbId, Origin = picked.Origin, Retries = picked.Retries + 1, ExpiresAt = DateTime.Now.AddHours(1) };
        await storage.ChangeReceiveStateAsync(afterFailure, StatusName.Failed);
        await storage.ChangeReceiveStateToImmediateRetryAsync(new[] { picked.DbId });

        (await storage.TryLeaseReceivedMessageAsync(picked, Lease)).Should().BeFalse();
    }

    [Fact]
    public async Task TryLease_GrantsToExactlyOneCaller_WhenCalledConcurrently()
    {
        var storage = CreateStorage();
        var message = await storage.StoreReceivedMessageAsync("test.topic", "group", CreateMessage());
        await storage.ChangeReceiveStateToImmediateRetryAsync(new[] { message.DbId });
        var copies = Enumerable.Range(0, 8)
            .Select(_ => new MediumMessage { DbId = message.DbId, Origin = message.Origin, StatusName = StatusName.RetryImmediately })
            .ToList();

        var results = await Task.WhenAll(copies.Select(c => Task.Run(() => storage.TryLeaseReceivedMessageAsync(c, Lease))));

        results.Count(r => r).Should().Be(1);
    }

    [Fact]
    public async Task ChangeReceiveState_KeepsTheLease_WhileTheDispatchStillOwnsIt()
    {
        var storage = CreateStorage();
        var picked = await StoreFlaggedAndReadAsync(storage);
        (await storage.TryLeaseReceivedMessageAsync(picked, Lease)).Should().BeTrue();

        // An intermediate Failed write from the executor's in-process retry loop carries the lease along.
        picked.Retries = 1;
        picked.ExpiresAt = DateTime.Now.AddHours(1);
        await storage.ChangeReceiveStateAsync(picked, StatusName.Failed);

        (await storage.GetReceivedMessagesOfNeedRetry(TimeSpan.Zero)).Select(m => m.DbId).Should().NotContain(picked.DbId);
    }

    [Fact]
    public async Task ChangeReceiveState_ClearsTheLease_WhenTheDispatchReleasedIt()
    {
        var storage = CreateStorage();
        var picked = await StoreFlaggedAndReadAsync(storage);
        (await storage.TryLeaseReceivedMessageAsync(picked, Lease)).Should().BeTrue();

        // The final Failed write of a dispatch comes with the lease dropped; the row is back on the Added-based pacing.
        picked.Retries = 1;
        picked.ExpiresAt = DateTime.Now.AddHours(1);
        picked.LeasedUntil = null;
        await storage.ChangeReceiveStateAsync(picked, StatusName.Failed);

        await Task.Delay(10);
        var failed = (await storage.GetReceivedMessagesOfNeedRetry(TimeSpan.Zero)).Single(m => m.DbId == picked.DbId);
        failed.StatusName.Should().Be(StatusName.Failed);
        failed.LeasedUntil.Should().BeNull();
        (await storage.GetReceivedMessagesOfNeedRetry(Lookback)).Select(m => m.DbId).Should().NotContain(picked.DbId);
    }

    [Fact]
    public async Task ChangeReceiveStateToImmediateRetry_LeavesSucceededMessageAlone()
    {
        var storage = CreateStorage();
        var message = await storage.StoreReceivedMessageAsync("test.topic", "group", CreateMessage());
        message.ExpiresAt = DateTime.Now.AddHours(1);
        await storage.ChangeReceiveStateAsync(message, StatusName.Succeeded);

        await storage.ChangeReceiveStateToImmediateRetryAsync(new[] { message.DbId });

        (await storage.GetReceivedMessagesOfNeedRetry(Lookback)).Select(m => m.DbId).Should().NotContain(message.DbId);
    }

    private static async Task<MediumMessage> StoreFlaggedAndReadAsync(IDataStorage storage)
    {
        var message = await storage.StoreReceivedMessageAsync("test.topic", "group", CreateMessage());
        await storage.ChangeReceiveStateToImmediateRetryAsync(new[] { message.DbId });
        return (await storage.GetReceivedMessagesOfNeedRetry(Lookback)).Single(m => m.DbId == message.DbId);
    }

    private static Message CreateMessage() => new(
        new Dictionary<string, string>
        {
            { Headers.MessageId, "1" },
            { Headers.MessageName, "test.topic" }
        },
        "payload");
}
