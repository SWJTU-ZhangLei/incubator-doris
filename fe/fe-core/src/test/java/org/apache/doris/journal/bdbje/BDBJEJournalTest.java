// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.journal.bdbje;

import org.apache.doris.catalog.Env;
// import org.apache.doris.journal.Journal;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
// import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class BDBJEJournalTest { // CHECKSTYLE IGNORE THIS LINE: BDBJE should use uppercase
    private static final Logger LOG = LogManager.getLogger(BDBJEJournalTest.class);
    private File tmpDir;

    @Before
    public void setUp() throws Exception {
        String dorisHome = System.getenv("DORIS_HOME");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(dorisHome));
        this.tmpDir = Files.createTempDirectory(Paths.get(dorisHome, "fe", "mocked"), "BDBJEJournalTest").toFile();
        LOG.debug("tmpDir path {}", tmpDir.getAbsolutePath());
        return;
    }

    @After
    public void cleanUp() throws Exception {
        FileUtils.deleteDirectory(tmpDir);
    }

    private int findValidPort() {
        int port = 0;
        for (int i = 0; i < 65535; i++) {
            try (ServerSocket socket = new ServerSocket(0)) {
                socket.setReuseAddress(true);
                port = socket.getLocalPort();
                try (DatagramSocket datagramSocket = new DatagramSocket(port)) {
                    datagramSocket.setReuseAddress(true);
                    break;
                } catch (SocketException e) {
                    LOG.info("The port {} is invalid and try another port", port);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Could not find a free TCP/IP port");
            }
        }
        return port;
    }

    @Test
    public void testSetup() throws Exception {
        int port = findValidPort();
        Preconditions.checkArgument(((port > 0) && (port < 65535)));
        // String selfNodeName = Env.genFeNodeName("127.0.0.1", port, false);
        LOG.info("BdbDir:{}", Env.getServingEnv().getBdbDir());
        // Journal journal = new BDBJEJournal(selfNodeName);
        // journal.open();
    }
}
