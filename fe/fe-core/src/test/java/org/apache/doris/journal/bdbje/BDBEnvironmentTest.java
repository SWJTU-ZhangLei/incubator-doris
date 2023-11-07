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
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

public class BDBEnvironmentTest {
    private static final Logger LOG = LogManager.getLogger(BDBEnvironmentTest.class);
    private static List<File> tmpDirs = new ArrayList<>();

    public static File createTmpDir() throws Exception {
        String dorisHome = System.getenv("DORIS_HOME");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(dorisHome));
        File dir = Files.createTempDirectory(Paths.get(dorisHome, "fe", "mocked"), "BDBEnvironmentTest").toFile();
        LOG.debug("createTmpDir path {}", dir.getAbsolutePath());
        tmpDirs.add(dir);
        return dir;
    }

    @AfterAll
    public static void cleanUp() throws Exception {
        for (File dir : tmpDirs) {
            LOG.info("deleteTmpDir path {}", dir.getAbsolutePath());
            // FileUtils.deleteDirectory(dir);
        }
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
        Preconditions.checkArgument(((port > 0) && (port < 65536)));
        return port;
    }

    private byte[] randomBytes() {
        byte[] byteArray = new byte[32];
        new SecureRandom().nextBytes(byteArray);
        return byteArray;
    }

    @Test
    public void testSetup() throws Exception {
        int port = findValidPort();
        String selfNodeName = Env.genFeNodeName("127.0.0.1", port, false);
        String selfNodeHostPort = "127.0.0.1:" + port;
        LOG.debug("selfNodeName:{}, selfNodeHostPort:{}", selfNodeName, selfNodeHostPort);

        BDBEnvironment bdbEnvironment = new BDBEnvironment();
        bdbEnvironment.setup(createTmpDir(), selfNodeName, selfNodeHostPort, selfNodeHostPort, true);

        String dbName = "testEnvironment";
        Database db = bdbEnvironment.openDatabase(dbName);
        DatabaseEntry key = new DatabaseEntry(randomBytes());
        DatabaseEntry value = new DatabaseEntry(randomBytes());

        Assertions.assertEquals(OperationStatus.SUCCESS, db.put(null, key, value));

        DatabaseEntry readValue = new DatabaseEntry();
        Assertions.assertEquals(OperationStatus.SUCCESS, db.get(null, key, readValue, LockMode.READ_COMMITTED));
        Assertions.assertEquals(new String(value.getData()), new String(readValue.getData()));

        // Remove database
        bdbEnvironment.removeDatabase(dbName);
        Exception exception = Assertions.assertThrows(IllegalStateException.class, () -> {
            db.put(null, key, value);
        });

        String expectedMessage = "Database was closed.";
        String actualMessage = exception.getMessage();
        LOG.debug("exception:", exception);
        Assertions.assertTrue(actualMessage.contains(expectedMessage));

        bdbEnvironment.close();
        exception = Assertions.assertThrows(IllegalStateException.class, () -> {
            db.put(null, key, value);
        });
        expectedMessage = "Environment is closed.";
        actualMessage = exception.getMessage();
        LOG.debug("exception:", exception);
        Assertions.assertTrue(actualMessage.contains(expectedMessage));
    }

    /**
     * Test build a BDBEnvironment cluster (1 master + 2 follower + 1 observer)
     * @throws Exception
     */
    @Test
    public void testCluster() throws Exception {
        int masterPort = findValidPort();
        String masterNodeName = Env.genFeNodeName("127.0.0.1", masterPort, false);
        String masterNodeHostPort = "127.0.0.1:" + masterPort;
        LOG.debug("masterNodeName:{}, masterNodeHostPort:{}", masterNodeName, masterNodeHostPort);

        BDBEnvironment masterEnvironment = new BDBEnvironment();
        File masterDir = createTmpDir();
        masterEnvironment.setup(masterDir, masterNodeName, masterNodeHostPort, masterNodeHostPort, true);

        List<BDBEnvironment> followerEnvironments = new ArrayList<>();
        List<File> followerDirs = new ArrayList<>();
        for (int i = 1; i <= 2; i++) {
            int followerPort = findValidPort();
            String followerNodeName = Env.genFeNodeName("127.0.0.1", followerPort, false);
            String followerNodeHostPort = "127.0.0.1:" + followerPort;
            LOG.debug("followerNodeName{}:{}, followerNodeHostPort{}:{}", i, i, followerNodeName, followerNodeHostPort);

            BDBEnvironment followerEnvironment = new BDBEnvironment();
            File followerDir = createTmpDir();
            followerDirs.add(followerDir);
            followerEnvironment.setup(followerDir, followerNodeName, followerNodeHostPort, masterNodeHostPort, true);
            followerEnvironments.add(followerEnvironment);
        }

        int observerPort = findValidPort();
        String observerNodeName = Env.genFeNodeName("127.0.0.1", observerPort, false);
        String observerNodeHostPort = "127.0.0.1:" + observerPort;
        LOG.debug("observerNodeName:{}, observerNodeHostPort:{}", observerNodeName, observerNodeHostPort);

        BDBEnvironment observerEnvironment = new BDBEnvironment();
        File observerDir = createTmpDir();
        observerEnvironment.setup(observerDir, observerNodeName, observerNodeHostPort, masterNodeHostPort, false);

        String dbName = "1234";
        Database masterDb = masterEnvironment.openDatabase(dbName);
        DatabaseEntry key = new DatabaseEntry(randomBytes());
        DatabaseEntry value = new DatabaseEntry(randomBytes());
        Assertions.assertEquals(OperationStatus.SUCCESS, masterDb.put(null, key, value));

        for (BDBEnvironment followerEnvironment : followerEnvironments) {
            Assertions.assertEquals(1, followerEnvironment.getDatabaseNames().size());
            Database followerDb = followerEnvironment.openDatabase(dbName);
            DatabaseEntry readValue = new DatabaseEntry();
            Assertions.assertEquals(OperationStatus.SUCCESS, followerDb.get(null, key, readValue, LockMode.READ_COMMITTED));
            Assertions.assertEquals(new String(value.getData()), new String(readValue.getData()));
        }

        Assertions.assertEquals(1, observerEnvironment.getDatabaseNames().size());
        Database observerDb = observerEnvironment.openDatabase(dbName);
        DatabaseEntry readValue = new DatabaseEntry();
        Assertions.assertEquals(OperationStatus.SUCCESS, observerDb.get(null, key, readValue, LockMode.READ_COMMITTED));
        Assertions.assertEquals(new String(value.getData()), new String(readValue.getData()));

        observerEnvironment.close();
        followerEnvironments.stream().forEach(environment -> {
            environment.close(); });
        masterEnvironment.close();

        masterEnvironment.openReplicatedEnvironment(masterDir);
        for (int i = 0; i < 2; i++) {
            followerEnvironments.get(i).openReplicatedEnvironment(followerDirs.get(i));
        }
        observerEnvironment.openReplicatedEnvironment(observerDir);

        observerEnvironment.close();
        followerEnvironments.stream().forEach(environment -> {
            environment.close(); });
        masterEnvironment.close();
    }

    @Test
    public void testRollbackException() throws Exception {
        List<Pair<BDBEnvironment, File>> followersInfo = new ArrayList<>();

        int masterPort = findValidPort();
        String masterNodeName = Env.genFeNodeName("127.0.0.1", masterPort, false);
        String masterNodeHostPort = "127.0.0.1:" + masterPort;
        LOG.debug("masterNodeName:{}, masterNodeHostPort:{}", masterNodeName, masterNodeHostPort);

        BDBEnvironment masterEnvironment = new BDBEnvironment();
        File masterDir = createTmpDir();
        masterEnvironment.setup(masterDir, masterNodeName, masterNodeHostPort, masterNodeHostPort, true);
        followersInfo.add(Pair.of(masterEnvironment, masterDir));

        for (int i = 0; i < 2; i++) {
            int followerPort = findValidPort();
            String followerNodeName = Env.genFeNodeName("127.0.0.1", followerPort, false);
            String followerNodeHostPort = "127.0.0.1:" + followerPort;
            LOG.debug("followerNodeName{}:{}, followerNodeHostPort{}:{}", i, i, followerNodeName, followerNodeHostPort);

            BDBEnvironment followerEnvironment = new BDBEnvironment();
            File followerDir = createTmpDir();
            followerEnvironment.setup(followerDir, followerNodeName, followerNodeHostPort, masterNodeHostPort, true);
            followersInfo.add(Pair.of(followerEnvironment, followerDir));
            // LOG.info("followerEnvironment state:{}", followerEnvironment.getReplicatedEnvironment().getState());
        }

        String beginDbName = String.valueOf(0L);
        Database masterDb = masterEnvironment.openDatabase(beginDbName);
        DatabaseEntry key = new DatabaseEntry(randomBytes());
        DatabaseEntry value = new DatabaseEntry(randomBytes());
        Assertions.assertEquals(OperationStatus.SUCCESS, masterDb.put(null, key, value));
        Assertions.assertEquals(1, masterEnvironment.getDatabaseNames().size());
        LOG.info("masterDir first is {}", masterDir.getAbsolutePath());

        followersInfo.stream().forEach(entryPair -> {
            Assertions.assertEquals(1, entryPair.first.getDatabaseNames().size());
            Database followerDb = entryPair.first.openDatabase(beginDbName);
            DatabaseEntry readValue = new DatabaseEntry();
            Assertions.assertEquals(OperationStatus.SUCCESS, followerDb.get(null, key, readValue, LockMode.READ_COMMITTED));
            Assertions.assertEquals(new String(value.getData()), new String(readValue.getData()));
        });

        followersInfo.stream().forEach(entryPair -> {
            entryPair.first.close();
        });

        Thread.sleep(1000);

        // all follower closed
        for (Pair<BDBEnvironment, File> entryPair : followersInfo) {
            File followerCopyDir = new File(entryPair.second.getAbsolutePath() + "_copy");
            LOG.info("Copy from {} to {}", entryPair.second.getAbsolutePath(), followerCopyDir);
            FileUtils.copyDirectory(entryPair.second, followerCopyDir);
        }

        followersInfo.stream().forEach(entryPair -> {
            entryPair.first.openReplicatedEnvironment(entryPair.second);
        });

        masterEnvironment = null;
        for (Pair<BDBEnvironment, File> entryPair : followersInfo) {
            if (entryPair.first.getReplicatedEnvironment().getState().equals(ReplicatedEnvironment.State.MASTER)) {
                masterEnvironment = entryPair.first;
                masterDir = entryPair.second;
                LOG.info("masterDir transfer to {}", entryPair.second.getAbsolutePath());
            }
        }
        Assertions.assertNotNull(masterEnvironment);

        masterDb = masterEnvironment.openDatabase(String.valueOf(1L));
        for (int i = 0; i < Config.txn_rollback_limit + 10; i++) {
            OperationStatus status = masterDb.put(null, new DatabaseEntry(randomBytes()), new DatabaseEntry(randomBytes()));
            Assertions.assertEquals(OperationStatus.SUCCESS, status);
        }
        Assertions.assertEquals(2, masterEnvironment.getDatabaseNames().size());
        Assertions.assertEquals(0, masterEnvironment.getDatabaseNames().get(0));
        Assertions.assertEquals(1, masterEnvironment.getDatabaseNames().get(1));

        followersInfo.stream().forEach(entryPair -> {
            entryPair.first.close();
        });

        // Restore follower's (not new master) bdbje dir
        for (Pair<BDBEnvironment, File> entryPair : followersInfo) {
            if (entryPair.second.equals(masterDir)) {
                continue;
            }
            LOG.info("Delete followerDir {} ", entryPair.second);
            FileUtils.deleteDirectory(entryPair.second);
            File followerCopyDir = new File(entryPair.second.getAbsolutePath() + "_copy");
            LOG.info("Move {} to {}", followerCopyDir, entryPair.second);
            FileUtils.moveDirectory(followerCopyDir, entryPair.second);
        }

        for (Pair<BDBEnvironment, File> entryPair : followersInfo) {
            if (entryPair.second.equals(masterDir)) {
                continue;
            }
            entryPair.first.openReplicatedEnvironment(entryPair.second);
        }

        BDBEnvironment newMasterEnvironment = null;
        boolean found = false;
        for (int i = 0; i < 300; i++) {
            for (Pair<BDBEnvironment, File> entryPair : followersInfo) {
                if (entryPair.second.equals(masterDir)) {
                    continue;
                }
                LOG.info("state: {}", entryPair.first.getReplicatedEnvironment().getState());
                if (entryPair.first.getReplicatedEnvironment().getState().equals(ReplicatedEnvironment.State.MASTER)) {
                    newMasterEnvironment = entryPair.first;
                    found = true;
                    break;
                }
            }
            if (found) {
                break;
            }
            Thread.sleep(1000);
        }
        Assertions.assertNotNull(newMasterEnvironment);

        masterDb = newMasterEnvironment.openDatabase(beginDbName);
        Assertions.assertEquals(OperationStatus.SUCCESS, masterDb.put(null, new DatabaseEntry(randomBytes()), new DatabaseEntry(randomBytes())));
        Assertions.assertEquals(1, newMasterEnvironment.getDatabaseNames().size());

        // old master
        masterEnvironment.openReplicatedEnvironment(masterDir);
        followersInfo.stream().forEach(entryPair -> {
            entryPair.first.close();
        });
    }
}
