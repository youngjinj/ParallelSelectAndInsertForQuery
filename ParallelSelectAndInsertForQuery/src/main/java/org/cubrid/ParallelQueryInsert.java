package org.cubrid;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.logging.Level;
import java.util.logging.Logger;

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

/*-
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
	
	...
	
	select count(*) from (select * from t1 intersect select * from t2); -- 100000
	select count(*) from (select * from t1 intersect select * from t3); -- 100000
	select count(*) from (select * from t1 intersect select * from t4_1); -- 100000
 */

/*-
	echo "java_stored_procedure=y" >> $CUBRID/conf/cubrid.conf
	
	cubrid server restart demodb
	
	create or replace procedure parallel_batch(num_thread int) as language java
	name 'ParallelQueryInsert.ParallelQueryInsertBatch(int)';
	
	call parallel_batch (12);
	
	tail -f ${HOME}/java#.log
*/

public class ParallelQueryInsert {
	private static final Logger LOGGER = Logger.getLogger(ParallelQueryInsert.class.getName());

	private static final String DB_URL = "jdbc:cubrid:192.168.2.205:33000:demodb:::";
	private static final String DB_USER = "dba";
	private static final String DB_PASSWORD = "";

	public static final int LABEL_COUNT = 2;
	public static final String LABEL_BEGIN = "begin";
	public static final String LABEL_END = "end";

	public static final String PREFIX_THREAD_ID = "__t_";
	public static final int NOT_USED = 0;
	public static final int USE_THREAD_ID = 1;

	static AtomicInteger atomicThreadId;

	public static void main(String[] args) {
		Instant start = Instant.now();

		ParallelQueryInsertBatch(12);

		Instant end = Instant.now();
		Duration duration = Duration.between(start, end);

		long seconds = duration.getSeconds();
		long absSeconds = Math.abs(seconds);
		String formattedTime = String.format("%02d:%02d:%02d", absSeconds / 3600, (absSeconds % 3600) / 60,
				absSeconds % 60);

		if (seconds < 0) {
			formattedTime = "-" + formattedTime;
		}

		LOGGER.log(Level.INFO, "Elapsed time: " + formattedTime);

	}

	public static void ParallelQueryInsertBatch(int numThreads) {
		{
			atomicThreadId = new AtomicInteger(1);
		}

		/*-
		 * BATCH-BEGIN
		 */

		LOGGER.log(Level.INFO, "BATCH Start.");

		/*-
		 * numRepeat1~4는 Thread에서 바인드 값의 한 쌍을 가져와서 바인드를 하는 횟수를 의미합니다.
		 * 
		 * 예를 들어,
		 * 쿼리에서 바인드가 필요한 'BETWEEN... AND...' 조건이 1개만 있다면 numRepeat는 1이 될 것입니다.
		 * 쿼리에서 바인드가 필요한 'BETWEEN... AND...' 조건이 2개가 있다면 numRepeat는 2가 될 것입니다.
		 * ...
		 * 쿼리에서 바인드가 필요한 'BETWEEN... AND...' 조건이 N개가 있다면 numRepeat는 N이 되어야 합니다.
		 * 
		 * 바인드 값의 리스트를 만들 때는
		 * 쿼리에서 바인드가 필요한 'BETWEEN... AND...' 조건의 개수에 관계 없이 한 쌍(2개)의 배열로 만들어야 합니다.
		 */

		/*-
		 * BATCH-1
		 */

		String query1 = "insert into t2 select * from t1 where c1 between ? and ?";
		// @formatter:off
		/*-
		 * 모든 바인드 값의 리스트는 'BETWEEN... AND...' 조건에 바인드 되는 한 쌍(2개)의 배열로 만들어야 합니다.
		 */
		String[][] valueList1 = {
				{ "A0000001", "A0100000" },
				{ "A0100001", "A0200000" },
				{ "A0200001", "A0300000" },
				{ "A0300001", "A0400000" },
				{ "A0400001", "A0500000" },
				{ "A0500001", "A0600000" },
				{ "A0600001", "A0700000" },
				{ "A0700001", "A0800000" },
				{ "A0800001", "A0900000" },
				{ "A0900001", "A1000000" }
			};
		// @formatter:on
		int numRepeat1 = 1;
		LOGGER.log(Level.INFO, "BATCH-1 Start.");
		ParallelQueryInsertInternal(query1, makeQueue(valueList1), numRepeat1, numThreads, NOT_USED);
		LOGGER.log(Level.INFO, "BATCH-1 Complete.");

		/*-
		 * BATCH-2
		 */

		String query2 = "insert into t3 select * from t1 where c1 between ? and ? and c1 between ? and ?";
		// @formatter:off
		/*-
		 * 모든 바인드 값의 리스트는 'BETWEEN... AND...' 조건에 바인드 되는 한 쌍(2개)의 배열로 만들어야 합니다.
		 */
		String[][] valueList2 = {
				{ "A0000001", "A0100000" },
				{ "A0100001", "A0200000" },
				{ "A0200001", "A0300000" },
				{ "A0300001", "A0400000" },
				{ "A0400001", "A0500000" },
				{ "A0500001", "A0600000" },
				{ "A0600001", "A0700000" },
				{ "A0700001", "A0800000" },
				{ "A0800001", "A0900000" },
				{ "A0900001", "A1000000" }
			};
		// @formatter:on
		int numRepeat2 = 2;
		LOGGER.log(Level.INFO, "BATCH-2 Start.");
		ParallelQueryInsertInternal(query2, makeQueue(valueList2), numRepeat2, numThreads, NOT_USED);
		LOGGER.log(Level.INFO, "BATCH-2 Complete.");

		/*-
		 * BATCH-3
		 */

		String query3 = "insert into t4 select ?, t.* from t1 t where t.c1 between ? and ?";
		// @formatter:off
		/*-
		 * 모든 바인드 값의 리스트는 'BETWEEN... AND...' 조건에 바인드 되는 한 쌍(2개)의 배열로 만들어야 합니다.
		 */
		String[][] valueList3 = {
				{ "A0000001", "A0100000" },
				{ "A0100001", "A0200000" },
				{ "A0200001", "A0300000" },
				{ "A0300001", "A0400000" },
				{ "A0400001", "A0500000" },
				{ "A0500001", "A0600000" },
				{ "A0600001", "A0700000" },
				{ "A0700001", "A0800000" },
				{ "A0800001", "A0900000" },
				{ "A0900001", "A1000000" }
			};
		// @formatter:on
		int numRepeat3 = 1;
		LOGGER.log(Level.INFO, "BATCH-3 Start.");
		ParallelQueryInsertInternal(query3, makeQueue(valueList3), numRepeat3, numThreads, USE_THREAD_ID);
		LOGGER.log(Level.INFO, "BATCH-3 Complete.");

		/*-
		 * BATCH-4
		 */

		String query4 = "insert into t4_1 select t.c1, t.c2 from t4 t where t.thread_id between ? and ?";
		// @formatter:off
		/*-
		 * Thread Id를 'BETWEEN... AND...' 조건으로 사용하시려면,
		 * 바인드 값의 리스트를 만들 때 INSERT 했던 Thread Id의 범위와 동일해야 합니다.
		 * 
		 * 예를 들어,
		 * BATCH-3에서 numThreads를 12로 해서 INSERT 했다면, Thread Id는 1부터 12까지 INSERT 됩니다.
		 * BATCH-4에서 사용할 바인드 값의 리스트도 1부터 12까지 만들어야 합니다.
		 *         
		 * BATCH-4를 수행할 때 numThreads는 BATCH-3의 numThreads와 같지 않아도 됩니다.
		 * BATCH-3을 수행할 때는 numThreads를 12로 설정했지만,
		 * BATCH-4를 수행할 때는 numThreads를 11이나 13으로 해도 문제가 없습니다.
		 * 
		 * 단, 모든 바인드 값의 리스트는 'BETWEEN... AND...' 조건에 바인드 되는 한 쌍(2개)의 배열로 만들어야 합니다.
		 */
		String[][] valueList4 = {
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
		
		/*-
		 * 편의성을 위해서 makeThreadIdArray 함수를 제공합니다.
		 * makeThreadIdArray 함수를 사용하면, valueList4처럼 모든 값을 작성하지 않고, numThreads를 전달해서 자동으로 만들 수 있습니다.
		 * 
		 * Thread Id를 바인드 값의 리스트로 만드는 경우에는 makeQueue 함수를 사용하면 안 됩니다.
		 * PREFIX_THREAD_ID를 붙여주기 위해서 makeQueueWithThreadId 함수를 사용해야 합니다.
		 * 
		 */
		String[][] valueList5 = makeThreadIdArray(numThreads);
		// @formatter:on
		int numRepeat4 = 1;
		LOGGER.log(Level.INFO, "BATCH-4 Start.");
		ParallelQueryInsertInternal(query4, makeQueueWithThreadId(valueList5), numRepeat4, numThreads, NOT_USED);
		LOGGER.log(Level.INFO, "BATCH-4 Complete.");

		/*-
		 * BATCH-END
		 */

		LOGGER.log(Level.INFO, "BATCH-ALL Complete.");
	}

	private static String[][] makeThreadIdArray(int paramNumThreads) {
		int numThreads = checkNumThreads(paramNumThreads);

		List<List<String>> threadIdList = new ArrayList<>();
		for (int i = 0; i < numThreads; i++) {
			threadIdList.add(new ArrayList<>(Arrays.asList(String.valueOf(i + 1), String.valueOf(i + 1))));
		}

		String[][] threadIdArray = threadIdList.stream().map(list -> list.toArray(new String[list.size()]))
				.toArray(String[][]::new);

		LOGGER.log(Level.INFO, Arrays.deepToString(threadIdArray));

		return threadIdArray;
	}

	private static ConcurrentLinkedQueue<Map<String, String>> makeQueue(String[][] valueList) {
		ConcurrentLinkedQueue<Map<String, String>> queue = new ConcurrentLinkedQueue<Map<String, String>>();

		for (int i = 0; i < valueList.length; i++) {
			if (valueList[i].length == LABEL_COUNT) {
				Map<String, String> map = new HashMap<String, String>();
				map.put(LABEL_BEGIN, valueList[i][0]);
				map.put(LABEL_END, valueList[i][1]);
				queue.offer(map);
			}
		}

		return queue;
	}

	private static ConcurrentLinkedQueue<Map<String, String>> makeQueueWithThreadId(String[][] valueList) {
		ConcurrentLinkedQueue<Map<String, String>> queue = new ConcurrentLinkedQueue<Map<String, String>>();

		for (int i = 0; i < valueList.length; i++) {
			if (valueList[i].length == LABEL_COUNT) {
				Map<String, String> map = new HashMap<String, String>();
				map.put(LABEL_BEGIN, PREFIX_THREAD_ID + valueList[i][0]);
				map.put(LABEL_END, PREFIX_THREAD_ID + valueList[i][1]);
				queue.offer(map);
			}
		}

		return queue;
	}

	private static int checkNumThreads(int numThreads) {
		int availableProcessors = Runtime.getRuntime().availableProcessors();
		if (numThreads > availableProcessors) {
			return availableProcessors;
		} else if (numThreads < 0) {
			return 1;
		} else {
			return numThreads;
		}
	}

	public static void ParallelQueryInsertInternal(String query, ConcurrentLinkedQueue<Map<String, String>> queue,
			int paramNumRepeatBind, int paramNumThreads, int insertThreadId) {
		int numRepeatBind = 1;
		int numThreads = checkNumThreads(paramNumThreads);

		if (paramNumRepeatBind < 0) {
			numRepeatBind = 1;
		} else {
			numRepeatBind = paramNumRepeatBind;
		}

		try {
			Class.forName("cubrid.jdbc.driver.CUBRIDDriver");
		} catch (ClassNotFoundException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
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
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
			return;
		}

		List<InsertCallable> insertCallableList = new ArrayList<>();

		if (insertThreadId == USE_THREAD_ID) {
			for (int i = 0; i < numThreads; i++) {
				insertCallableList.add(new InsertCallable(connectionList.get(i), query, queue, numRepeatBind,
						atomicThreadId.getAndIncrement()));
			}
		} else {
			for (int i = 0; i < numThreads; i++) {
				insertCallableList.add(new InsertCallable(connectionList.get(i), query, queue, numRepeatBind));
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
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		} catch (ExecutionException e) {
			needRollback = true;
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
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

					LOGGER.log(Level.INFO, "commit");
				} catch (InterruptedException e) {
					needRollback = true;
					LOGGER.log(Level.SEVERE, e.getMessage(), e);
				} catch (ExecutionException e) {
					needRollback = true;
					LOGGER.log(Level.SEVERE, e.getMessage(), e);
				}

				commitExecutorService.shutdown();
				try {
					commitExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
				} catch (InterruptedException e) {
					LOGGER.log(Level.SEVERE, e.getMessage(), e);
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

					LOGGER.log(Level.INFO, "rollback");
				} catch (InterruptedException e) {
					needRollback = true;
					LOGGER.log(Level.SEVERE, e.getMessage(), e);
				} catch (ExecutionException e) {
					needRollback = true;
					LOGGER.log(Level.SEVERE, e.getMessage(), e);
				}

				rollbackExecutorService.shutdown();
				try {
					rollbackExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
				} catch (InterruptedException e) {
					LOGGER.log(Level.SEVERE, e.getMessage(), e);
				}
			}
		}

		insertExecutorService.shutdown();
		try {
			insertExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}

		try {
			for (Connection connection : connectionList) {
				if (connection != null) {
					connection.close();
				}
			}
		} catch (SQLException e) {
			LOGGER.log(Level.SEVERE, e.getMessage(), e);
		}
	}
}

class InsertCallable implements Callable<Void> {
	private static final Logger LOGGER = Logger.getLogger(InsertCallable.class.getName());

	private Connection connection;
	private String query;
	private ConcurrentLinkedQueue<Map<String, String>> queue;
	private int numRepeatBind;
	private String threadIdWithPrefix;

	public InsertCallable(Connection connection, String query, ConcurrentLinkedQueue<Map<String, String>> queue,
			int numRepeatBind) {
		this.connection = connection;
		this.query = query;
		this.queue = queue;
		this.numRepeatBind = numRepeatBind;
		this.threadIdWithPrefix = null;
	}

	public InsertCallable(Connection connection, String query, ConcurrentLinkedQueue<Map<String, String>> queue,
			int numRepeatBind, long threadId) {
		this.connection = connection;
		this.query = query;
		this.queue = queue;
		this.numRepeatBind = numRepeatBind;
		this.threadIdWithPrefix = ParallelQueryInsert.PREFIX_THREAD_ID + threadId;
	}

	@Override
	public Void call() throws SQLException {
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			while (!Thread.interrupted()) {

				Map<String, String> work = (queue != null) ? queue.poll() : null;

				if (work != null) {
					int i = 1;
					int max = numRepeatBind * ParallelQueryInsert.LABEL_COUNT;

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
				LOGGER.log(Level.INFO, "inserted " + count + " rows");
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