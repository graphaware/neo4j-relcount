package com.graphaware.relcount.common.internal.cache;

/**
 * Component that benefits from knowing that operations performed on it are part of a batch operation.
 */
public interface BatchFriendly {

    /**
     * Tell the component that subsequent operations are part of a batch. {@link #endBatchMode()} must be called before
     * this method is called again.
     *
     * @throws IllegalStateException if this method has been previously called and wasn't followed by a call to {@link #endBatchMode()}.
     */
    void startBatchMode();

    /**
     * Tell the component that the batch has been finished.
     *
     * @throws IllegalStateException if {@link #startBatchMode()} method has not been previously called.
     */
    void endBatchMode();
}
