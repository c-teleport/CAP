// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

namespace DotNetCore.CAP.Internal;

/// <summary>
/// The message status name.
/// </summary>
public enum StatusName
{
    Failed = -1,
    Scheduled,
    Succeeded,

    Delayed,
    Queued,

    /// <summary>
    /// A received message that was consumed but could not finish executing before a graceful shutdown.
    /// The retry processor picks these up immediately, bypassing the <c>FallbackWindowLookbackSeconds</c>
    /// window, because the shutting-down instance was the sole owner of the message.
    /// </summary>
    RetryImmediately
}