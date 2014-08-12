package org.apache.airavata.gfac.jclouds.exceptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by root on 8/12/14.
 */
public class RunScriptFailureException extends Exception{
    private static final Logger log = LoggerFactory.getLogger(RunScriptFailureException.class);

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public RunScriptFailureException(String s) {
        super(s);
    }

    public RunScriptFailureException(Exception e) {
        super(e);
        log.error(e.getMessage(),e);
    }

    public RunScriptFailureException(String s, Throwable throwable) {
        super(s, throwable);
        log.error(s,throwable);
    }
}
