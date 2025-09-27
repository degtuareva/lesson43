package dao;

import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import entity.Task;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TaskDaoTest {
  private TaskDao taskDao;

  @BeforeAll
  public void setUp() {
    try {
      DataSource dataSource = EmbeddedPostgres
              .builder()
              .start()
              .getPostgresDatabase();

      initializeDb(dataSource);
      taskDao = new TaskDao(dataSource);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeEach
  public void beforeEach() {
    taskDao.deleteAll();
  }

  private void initializeDb(DataSource dataSource) {
    try (InputStream inputStream = this.getClass().getResource("/initial.sql").openStream()) {
      String sql = new String(inputStream.readAllBytes());
      try (Connection connection = dataSource.getConnection();
           Statement statement = connection.createStatement()) {
        statement.executeUpdate(sql);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testSaveSetsId() {
    Task task = new Task("test task", false, LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS));
    taskDao.save(task);
    assertThat(task.getId()).isNotNull();
  }

  @Test
  public void testFindAllReturnsAllTasks() {
    Task firstTask = new Task("first task", false, LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS));
    taskDao.save(firstTask);

    Task secondTask = new Task("second task", false, LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS));
    taskDao.save(secondTask);

    assertThat(taskDao.findAll())
            .hasSize(2)
            .extracting("id")
            .contains(firstTask.getId(), secondTask.getId());
  }

  @Test
  public void testDeleteAllDeletesAllRowsInTasks() {
    Task firstTask = new Task("any task", false, LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS));
    taskDao.save(firstTask);

    int rowsDeleted = taskDao.deleteAll();
    assertThat(rowsDeleted).isEqualTo(1);

    assertThat(taskDao.findAll()).isEmpty();
  }

  @Test
  public void testGetByIdReturnsCorrectTask() {
    Task task = new Task("test task", false, LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS));
    taskDao.save(task);

    Task fetched = taskDao.getById(task.getId());
    assertThat(fetched).isNotNull();

    assertThat(fetched.getId()).isEqualTo(task.getId());
    assertThat(fetched.getTitle()).isEqualTo(task.getTitle());
    assertThat(fetched.getFinished()).isEqualTo(task.getFinished());
    assertThat(fetched.getCreatedDate()).isEqualTo(task.getCreatedDate());
  }

  @Test
  public void testFindNotFinishedReturnsCorrectTasks() {
    Task unfinishedTask = new Task("test task", false, LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS));
    taskDao.save(unfinishedTask);

    Task finishedTask = new Task("test task", true, LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS));
    taskDao.save(finishedTask);

    assertThat(taskDao.findAllNotFinished())
            .singleElement()
            .satisfies(t -> {
              assertThat(t.getId()).isEqualTo(unfinishedTask.getId());
              assertThat(t.getTitle()).isEqualTo(unfinishedTask.getTitle());
              assertThat(t.getFinished()).isEqualTo(unfinishedTask.getFinished());
              assertThat(t.getCreatedDate()).isEqualTo(unfinishedTask.getCreatedDate());
            });
  }

  @Test
  public void testFindNewestTasksReturnsCorrectTasks() {
    Task firstTask = new Task("first task", false, LocalDateTime.now().minusDays(2).truncatedTo(ChronoUnit.MILLIS));
    taskDao.save(firstTask);

    Task secondTask = new Task("second task", false, LocalDateTime.now().minusDays(1).truncatedTo(ChronoUnit.MILLIS));
    taskDao.save(secondTask);

    Task thirdTask = new Task("third task", false, LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS));
    taskDao.save(thirdTask);

    List<Task> newestTasks = taskDao.findNewestTasks(2);
    assertThat(newestTasks)
            .hasSize(2)
            .extracting("id")
            .containsExactlyInAnyOrder(secondTask.getId(), thirdTask.getId());
  }

  @Test
  public void testFinishSetsCorrectFlagInDb() {
    Task task = new Task("test task", false, LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS));
    taskDao.save(task);

    Task finishedTask = taskDao.finishTask(task);
    assertThat(finishedTask.getFinished()).isTrue();

    Task fetched = taskDao.getById(task.getId());
    assertThat(fetched.getFinished()).isTrue();
  }

  @Test
  public void deleteByIdDeletesOnlyNecessaryData() {
    Task taskToDelete = new Task("first task", false, LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS));
    taskDao.save(taskToDelete);

    Task taskToPreserve = new Task("second task", false, LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS));
    taskDao.save(taskToPreserve);

    taskDao.deleteById(taskToDelete.getId());

    assertThat(taskDao.getById(taskToDelete.getId())).isNull();
    assertThat(taskDao.getById(taskToPreserve.getId())).isNotNull();
  }
}
