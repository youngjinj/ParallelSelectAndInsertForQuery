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

public class ParallelQueryInsert {
	public static final String LABEL_BEGIN = "begin";
	public static final String LABEL_END = "end";

	private static final String DB_URL = "jdbc:cubrid:192.168.2.205:33000:demodb:::";
	private static final String DB_USER = "dba";
	private static final String DB_PASSWORD = "";

	public static void main(String[] args) {

		Instant start = Instant.now();

		/*-
		echo "java_stored_procedure=y" >> $CUBRID/conf/cubrid.conf
		 
		create or replace procedure parallel_insert([query] varchar, thread_num int) as language java
		name 'ParallelQueryInsert.ParallelQueryInsertInternal(java.lang.String, int)';
		
		drop table if exists t1, t2;
		create table t1 (c1 varchar primary key, c2 varchar);
		insert into t1 select 'A' || lpad (rownum, 7, '0'), 'B' || lpad (rownum, 7, '0') from db_class a, db_class b, db_class c, db_class d, db_class e limit 1000000;
		create table t2 like t1;
		
		call parallel_insert('insert into t2 select * from t1 where c1 between ? and ?', 10);
		 */

		String query = "insert into t2 select * from t1 where c1 between ? and ?";
		int threadNum = 100;
		ParallelQueryInsertInternal(query, threadNum);

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

	public static void ParallelQueryInsertInternal(String paramQuery, int paramNumThreads) {
		/*-
		 * Bind variables 
		 */
		ConcurrentLinkedQueue<Map<String, String>> queue = new ConcurrentLinkedQueue<Map<String, String>>() {
			{
				offer(new HashMap<String, String>() {
					{
						put(LABEL_BEGIN, "A0000001");
						put(LABEL_END, "A0100000");
					}
				});
				offer(new HashMap<String, String>() {
					{
						put(LABEL_BEGIN, "A0100001");
						put(LABEL_END, "A0200000");
					}
				});
				offer(new HashMap<String, String>() {
					{
						put(LABEL_BEGIN, "A0200001");
						put(LABEL_END, "A0300000");
					}
				});
				offer(new HashMap<String, String>() {
					{
						put(LABEL_BEGIN, "A0300001");
						put(LABEL_END, "A0400000");
					}
				});
				offer(new HashMap<String, String>() {
					{
						put(LABEL_BEGIN, "A0400001");
						put(LABEL_END, "A0500000");
					}
				});
				offer(new HashMap<String, String>() {
					{
						put(LABEL_BEGIN, "A0500001");
						put(LABEL_END, "A0600000");
					}
				});
				offer(new HashMap<String, String>() {
					{
						put(LABEL_BEGIN, "A0600001");
						put(LABEL_END, "A0700000");
					}
				});
				offer(new HashMap<String, String>() {
					{
						put(LABEL_BEGIN, "A0700001");
						put(LABEL_END, "A0800000");
					}
				});
				offer(new HashMap<String, String>() {
					{
						put(LABEL_BEGIN, "A0800001");
						put(LABEL_END, "A0900000");
					}
				});
				offer(new HashMap<String, String>() {
					{
						put(LABEL_BEGIN, "A0900001");
						put(LABEL_END, "A1000000");
					}
				});
			}
		};

		String query = paramQuery;
		int numThreads = 1;

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
		for (int i = 0; i < numThreads; i++) {
			insertCallableList.add(new InsertCallable(connectionList.get(i), query, queue));
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

	public InsertCallable(Connection connection, String query, ConcurrentLinkedQueue<Map<String, String>> queue) {
		this.connection = connection;
		this.query = query;
		this.queue = queue;
	}

	@Override
	public Void call() throws SQLException {
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			while (!Thread.interrupted()) {
				Map<String, String> work = queue.poll();

				if (work != null) {
					statement.setString(1, work.get(ParallelQueryInsert.LABEL_BEGIN));
					statement.setString(2, work.get(ParallelQueryInsert.LABEL_END));
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