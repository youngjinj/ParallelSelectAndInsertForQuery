package org.cubrid;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/*-
 * Settings to use the below in eclipse'
 *   - @formatter:off
 *   - @formatter:on
 * 
 * Window -> Preferences -> Java -> Code Style -> Formatter -> Active profile: -> Edit...
 *
 *     Profile name: "Eclipse [built-in]" -> "Eclipse [built-in] [off/on tags]"
 *     Off.On Tags: [v] Enable Off/On tags
 *
 * (Profile 'Eclipse [built-in]') -> OK
 * (Preferences) -> Apply and Close
 */

public class ParallelQueryInsert {
	public static final String PREFIX_THREAD_ID = "__t_";

	public static final int LABEL_COUNT = 2;
	public static final String LABEL_BEGIN = "begin";
	public static final String LABEL_END = "end";

	private static final String DB_URL = "jdbc:cubrid:192.168.2.205:33000:demodb:::";
	private static final String DB_USER = "dba";
	private static final String DB_PASSWORD = "";

	private static ConcurrentLinkedQueue<Map<String, String>> queue1;
	private static ConcurrentLinkedQueue<Map<String, String>> queue2;
	private static ConcurrentLinkedQueue<Map<String, String>> queue3;
	private static ConcurrentLinkedQueue<Map<String, String>> queueWithThreadId;

	static AtomicInteger atomicThreadId;

	public static void main(String[] args) {
		String query = null;
		int numRepeat = 1;
		int numThreads = 1;

		Instant start = Instant.now();

		/*-
		echo "java_stored_procedure=y" >> $CUBRID/conf/cubrid.conf
		 
		create or replace procedure parallel_insert([query] varchar, thread_num int) as language java
		name 'ParallelQueryInsert.ParallelQueryInsertInternal(java.lang.String, int)';
		
		drop table if exists t1, t2, t3, t4, t4_1;
		create table t1 (c1 varchar primary key, c2 varchar);
		insert into t1 select 'A' || lpad (rownum, 7, '0'), 'B' || lpad (rownum, 7, '0') from db_class a, db_class b, db_class c, db_class d, db_class e limit 1000000;
		create table t2 like t1;
		create table t3 like t1;
		create table t4 like t1;
		alter table t4 add column thread_id varchar not null first;
		create table t4_1 like t1;
		
		truncate table t2;
		truncate table t3;
		truncate table t4;
		truncate table t4_1;
		
		call parallel_insert('insert into t2 select * from t1 where c1 between ? and ?', 10);
		...
		
		select count(*) from (select * from t1 intersect select * from t2);
		select count(*) from (select * from t1 intersect select * from t3);
		select count(*) from (select * from t1 intersect select * from t4_1);
		 */

		numThreads = 100;

		// @formatter:off
		query = "insert into t2 select * from t1 where c1 between ? and ?";
		numRepeat = 1;
		// ParallelQueryInsertInternal(query, queue1, numRepeat, numThreads, false);

		query = "insert into t3 select * from t1 where c1 between ? and ? and c1 between ? and ?";
		numRepeat = 2;
		// ParallelQueryInsertInternal(query, queue2, numRepeat, numThreads, false);

		query = "insert into t4 select ?, t.* from t1 t where t.c1 between ? and ?";
		numRepeat = 1;
		ParallelQueryInsertInternal(query, queue3, numRepeat, numThreads, true);

		query = "insert into t4_1 select t.c1, t.c2 from t4 t where t.thread_id between ? and ?";
		numRepeat = 1;
		ParallelQueryInsertInternal(query, queueWithThreadId, numRepeat, numThreads, false);
		// @formatter:on

		Instant end = Instant.now();
		Duration duration = Duration.between(start, end);

		long seconds = duration.getSeconds();
		long absSeconds = Math.abs(seconds);
		String formattedTime = String.format("%02d:%02d:%02d", absSeconds / 3600, (absSeconds % 3600) / 60,
				absSeconds % 60);

		if (seconds < 0) {
			formattedTime = "-" + formattedTime;
		}
		System.out.println("");
		System.out.println("Elapsed time: " + formattedTime);
		System.out.println("");
	}

	static {
		queue1 = new ConcurrentLinkedQueue<Map<String, String>>();
		queue2 = new ConcurrentLinkedQueue<Map<String, String>>();
		queue3 = new ConcurrentLinkedQueue<Map<String, String>>();
		queueWithThreadId = new ConcurrentLinkedQueue<Map<String, String>>();

		makeQueue1();
		makeQueue2();
		makeQueue3();
		makeQueue4();

		atomicThreadId = new AtomicInteger(1);
	}

	private static void makeQueue1() {
		// @formatter:off
		String[][] valueList = {
				{ "A0000001", "A0100000" },
				{ "A0100001", "A0200000" },
				{ "A0200001", "A0300000" },
				{ "A0300001", "A0400000" },
				{ "A0400001", "A0500000" },
				{ "A0500001", "A0600000" },
				{ "A0600001", "A0700000" },
				{ "A0700001", "A0800000" },
				{ "A0800001", "A0900000" },
				{ "A0900001", "A1000000" } };
		// @formatter:on

		for (int i = 0; i < valueList.length; i++) {
			if (valueList[i].length == LABEL_COUNT) {
				Map<String, String> map = new HashMap<String, String>();
				map.put(LABEL_BEGIN, valueList[i][0]);
				map.put(LABEL_END, valueList[i][1]);
				queue1.offer(map);
			}
		}
	}

	private static void makeQueue2() {
		// @formatter:off
		String[][] valueList = {
				{ "A0000001", "A0100000" },
				{ "A0100001", "A0200000" },
				{ "A0200001", "A0300000" },
				{ "A0300001", "A0400000" },
				{ "A0400001", "A0500000" },
				{ "A0500001", "A0600000" },
				{ "A0600001", "A0700000" },
				{ "A0700001", "A0800000" },
				{ "A0800001", "A0900000" },
				{ "A0900001", "A1000000" } };
		// @formatter:on

		for (int i = 0; i < valueList.length; i++) {
			if (valueList[i].length == LABEL_COUNT) {
				Map<String, String> map = new HashMap<String, String>();
				map.put(LABEL_BEGIN, valueList[i][0]);
				map.put(LABEL_END, valueList[i][1]);
				queue2.offer(map);
			}
		}
	}

	private static void makeQueue3() {
		// @formatter:off
		String[][] valueList = {
				{ "A0000001", "A0100000" },
				{ "A0100001", "A0200000" },
				{ "A0200001", "A0300000" },
				{ "A0300001", "A0400000" },
				{ "A0400001", "A0500000" },
				{ "A0500001", "A0600000" },
				{ "A0600001", "A0700000" },
				{ "A0700001", "A0800000" },
				{ "A0800001", "A0900000" },
				{ "A0900001", "A1000000" } };
		// @formatter:on

		for (int i = 0; i < valueList.length; i++) {
			if (valueList[i].length == LABEL_COUNT) {
				Map<String, String> map = new HashMap<String, String>();
				map.put(LABEL_BEGIN, valueList[i][0]);
				map.put(LABEL_END, valueList[i][1]);
				queue3.offer(map);
			}
		}
	}

	private static void makeQueue4() {
		// @formatter:off
		String[][] valueList = {
				{ "1", "1" },
				{ "2", "2" },
				{ "3", "3" },
				{ "4", "4" },
				{ "5", "5" },
				{ "6", "6" },
				{ "7", "7" },
				{ "8", "8" },
				{ "9", "9" },
				{ "10", "10" },
				{ "11", "11" },
				{ "12", "12" } };
		// @formatter:on

		for (int i = 0; i < valueList.length; i++) {
			if (valueList[i].length == LABEL_COUNT) {
				Map<String, String> map = new HashMap<String, String>();
				map.put(LABEL_BEGIN, PREFIX_THREAD_ID + valueList[i][0]);
				map.put(LABEL_END, PREFIX_THREAD_ID + valueList[i][1]);
				queueWithThreadId.offer(map);
			}
		}
	}

	public static void ParallelQueryInsertInternal(String query, ConcurrentLinkedQueue<Map<String, String>> queue,
			int paramNumRepeat, int paramNumThreads, boolean insertThreadId) {
		int numRepeat = 1;
		int numThreads = 1;

		if (paramNumRepeat < 0) {
			numRepeat = 1;
		} else {
			numRepeat = paramNumRepeat;
		}

		int availableProcessors = Runtime.getRuntime().availableProcessors();
		if (paramNumThreads > availableProcessors) {
			numThreads = availableProcessors;
		} else if (paramNumThreads < 0) {
			numThreads = 1;
		} else {
			numThreads = paramNumThreads;
		}

		try {
			Class.forName("cubrid.jdbc.driver.CUBRIDDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return;
		}

		List<Connection> connectionList = new ArrayList<Connection>();
		try {
			for (int i = 0; i < numThreads; i++) {
				Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
				connection.setAutoCommit(false);
				connectionList.add(connection);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return;
		}

		List<InsertCallable> insertCallableList = new ArrayList<>();

		if (insertThreadId) {
			for (int i = 0; i < numThreads; i++) {
				insertCallableList.add(new InsertCallable(connectionList.get(i), query, queue, numRepeat, atomicThreadId.getAndIncrement()));
			}
		} else {
			for (int i = 0; i < numThreads; i++) {
				insertCallableList.add(new InsertCallable(connectionList.get(i), query, queue, numRepeat));
			}
		}

		boolean needRollback = false;

		ExecutorService insertExecutorService = Executors.newFixedThreadPool(numThreads);
		List<Future<Void>> insertFutureList = null;
		try {
			insertFutureList = insertExecutorService.invokeAll(insertCallableList);

			for (Future<Void> future : insertFutureList) {

				future.get();
			}
		} catch (InterruptedException e) {
			needRollback = true;
			e.printStackTrace();
		} catch (ExecutionException e) {
			needRollback = true;
			e.printStackTrace();
		}

		if (connectionList.size() > 0) {
			if (!needRollback) {
				List<CommitCallable> commitCallableList = new ArrayList<>();
				for (int i = 0; i < numThreads; i++) {
					commitCallableList.add(new CommitCallable(connectionList.get(i)));
				}

				ExecutorService commitExecutorService = Executors.newFixedThreadPool(numThreads);
				List<Future<Void>> commitFutureList = null;
				try {
					commitFutureList = commitExecutorService.invokeAll(commitCallableList);

					for (Future<Void> future : commitFutureList) {
						future.get();
					}

					System.out.println("commit");
				} catch (InterruptedException e) {
					needRollback = true;
					e.printStackTrace();
				} catch (ExecutionException e) {
					needRollback = true;
					e.printStackTrace();
				}

				commitExecutorService.shutdown();
				try {
					commitExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			if (needRollback) {
				List<RollbackCallable> rollbackCallableList = new ArrayList<>();
				for (int i = 0; i < numThreads; i++) {
					rollbackCallableList.add(new RollbackCallable(connectionList.get(i)));
				}

				ExecutorService rollbackExecutorService = Executors.newFixedThreadPool(numThreads);
				List<Future<Void>> rollbackFutureList = null;
				try {
					rollbackFutureList = rollbackExecutorService.invokeAll(rollbackCallableList);

					for (Future<Void> future : rollbackFutureList) {
						future.get();
					}

					System.out.println("rollback");
				} catch (InterruptedException e) {
					needRollback = true;
					e.printStackTrace();
				} catch (ExecutionException e) {
					needRollback = true;
					e.printStackTrace();
				}

				rollbackExecutorService.shutdown();
				try {
					rollbackExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		insertExecutorService.shutdown();
		try {
			insertExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		try {
			for (Connection connection : connectionList) {
				if (connection != null) {
					connection.close();
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}

class InsertCallable implements Callable<Void> {
	private Connection connection;
	private String query;
	private ConcurrentLinkedQueue<Map<String, String>> queue;
	private int numRepeat;
	private String threadIdWithPrefix;

	public InsertCallable(Connection connection, String query, ConcurrentLinkedQueue<Map<String, String>> queue,
			int numRepeat) {
		this.connection = connection;
		this.query = query;
		this.queue = queue;
		this.numRepeat = numRepeat;
		this.threadIdWithPrefix = null;
	}

	public InsertCallable(Connection connection, String query, ConcurrentLinkedQueue<Map<String, String>> queue,
			int numRepeat, long threadId) {
		this.connection = connection;
		this.query = query;
		this.queue = queue;
		this.numRepeat = numRepeat;
		this.threadIdWithPrefix = ParallelQueryInsert.PREFIX_THREAD_ID + threadId;
	}

	@Override
	public Void call() throws SQLException {
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			while (!Thread.interrupted()) {
				Map<String, String> work = queue.poll();

				if (work != null) {
					int i = 1;
					int max = numRepeat * ParallelQueryInsert.LABEL_COUNT;

					if (threadIdWithPrefix != null) {
						statement.setString(i, threadIdWithPrefix);
						i++;
						max++;
					}

					for (; i < max; i += ParallelQueryInsert.LABEL_COUNT) {
						statement.setString(i, work.get(ParallelQueryInsert.LABEL_BEGIN));
						statement.setString(i + 1, work.get(ParallelQueryInsert.LABEL_END));
					}
				} else {
					break;
				}

				int count = 0;
				count = statement.executeUpdate();
				System.out.println("inserted " + count + " rows");
			}
		} catch (SQLException e) {
			throw e;
		}

		return null;
	}
};

class CommitCallable implements Callable<Void> {
	private Connection connection;

	public CommitCallable(Connection connection) {
		this.connection = connection;
	}

	@Override
	public Void call() throws SQLException {
		try {
			connection.commit();
		} catch (SQLException e) {
			throw e;
		}

		return null;
	}
};

class RollbackCallable implements Callable<Void> {
	private Connection connection;

	public RollbackCallable(Connection connection) {
		this.connection = connection;
	}

	@Override
	public Void call() throws SQLException {
		try {
			connection.rollback();
		} catch (SQLException e) {
			throw e;
		}

		return null;
	}
};