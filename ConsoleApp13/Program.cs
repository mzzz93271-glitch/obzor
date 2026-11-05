using System.Collections.Concurrent;

namespace SynchronizationPrimitives;

internal class Program
{
    static async Task Main()
    {
        Console.WriteLine("\n=== Примитивы синхронизации ===\n");
        Console.WriteLine("1. SemaphoreSlim");
        Console.WriteLine("2. Producer-Consumer");
        Console.WriteLine("3. Mutex");
        Console.WriteLine("4. Сравнение lock и Interlocked");
        Console.Write("\nВыберите эксперимент: ");

        string? choice = Console.ReadLine();

        switch (choice)
        {
            case "1":
                {
                    var demo = new SemaphoreDemo();
                    await demo.RunAsync();
                    break;
                }

            case "2":
                {
                    using var demo = new ProducerConsumerDemo();
                    await demo.RunAsync();
                    break;
                }

            case "3":
                MutexDemo.Run();
                break;

            case "4":
                await CounterDemo.RunAsync();
                break;

            default:
                Console.WriteLine("Неизвестная команда.");
                break;
        }
    }
}

public class SemaphoreDemo
{
    // Чтобы проверить лимит в 2 задачи, замените оба значения 3 на 2.
    private readonly SemaphoreSlim _semaphore = new(3, 3);

    private int _activeTasks;
    private int _maxActiveTasks;

    public async Task RunAsync(
        CancellationToken cancellationToken = default,
        TimeSpan? waitTimeout = null)
    {
        Console.WriteLine("=== SEMAPHORE DEMO ===");

        Task[] tasks = Enumerable.Range(1, 12)
            .Select(id => ProcessAsync(id, cancellationToken, waitTimeout))
            .ToArray();

        try
        {
            await Task.WhenAll(tasks);
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Обработка отменена.");
        }
        catch (Exception ex)
        {
            Console.WriteLine(
                $"Ошибка выполнения: {ex.GetBaseException().Message}");
        }

        Console.WriteLine();
        Console.WriteLine($"Максимум одновременных задач: {_maxActiveTasks}");
        Console.WriteLine($"Свободных разрешений: {_semaphore.CurrentCount}");

        if (!cancellationToken.IsCancellationRequested)
        {
            Console.WriteLine();
            Console.WriteLine("Проверка повторного использования:");

            await ProcessAsync(99, CancellationToken.None, waitTimeout);

            Console.WriteLine("Проверка завершена.");
        }
    }

    private async Task ProcessAsync(
        int id,
        CancellationToken cancellationToken,
        TimeSpan? waitTimeout)
    {
        Console.WriteLine($"Задача {id}: ожидает разрешения");

        bool acquired;

        if (waitTimeout is TimeSpan timeout)
        {
            acquired = await _semaphore.WaitAsync(timeout, cancellationToken);

            if (!acquired)
            {
                Console.WriteLine(
                    $"Задача {id}: не получила разрешение за " +
                    $"{timeout.TotalSeconds} сек.");
                return;
            }
        }
        else
        {
            await _semaphore.WaitAsync(cancellationToken);
            acquired = true;
        }

        // Освобождаем семафор только после успешного получения разрешения.
        try
        {
            int active = Interlocked.Increment(ref _activeTasks);
            UpdateMaximum(active);

            Console.WriteLine(
                $"Задача {id}: начала работу. Активно: {active}");

            await Task.Delay(
                Random.Shared.Next(500, 1500),
                cancellationToken);

            // Искусственная ошибка для проверки блока finally.
            if (id == 5)
            {
                throw new InvalidOperationException(
                    "Ошибка обработки задания №5");
            }

            Console.WriteLine($"Задача {id}: завершена");
        }
        finally
        {
            Interlocked.Decrement(ref _activeTasks);

            if (acquired)
            {
                _semaphore.Release();
                Console.WriteLine($"Задача {id}: освободила разрешение");
            }
        }
    }

    private void UpdateMaximum(int current)
    {
        int previous;

        do
        {
            previous = Volatile.Read(ref _maxActiveTasks);

            if (current <= previous)
            {
                return;
            }
        }
        while (Interlocked.CompareExchange(
                   ref _maxActiveTasks,
                   current,
                   previous) != previous);
    }
}

public class ProducerConsumerDemo : IDisposable
{
    private readonly ConcurrentQueue<int> _queue = new();
    private readonly AutoResetEvent _signal = new(false);

    private int _processedCount;

    private const int ProcessingDelayMs = 1000;

    public async Task RunAsync()
    {
        Console.WriteLine();
        Console.WriteLine("=== PRODUCER-CONSUMER DEMO ===");

        // WaitOne блокирует поток, поэтому потребитель работает через Task.Run.
        Task consumerTask = Task.Run(Consume);

        // Четыре производителя по десять элементов.
        Task[] producerTasks = Enumerable.Range(1, 4)
            .Select(producerId => Task.Run(() => Produce(producerId, 10)))
            .ToArray();

        await Task.WhenAll(producerTasks);

        // Специальное значение сообщает потребителю о завершении.
        _queue.Enqueue(-1);
        _signal.Set();

        await consumerTask;

        Console.WriteLine();
        Console.WriteLine($"Обработано элементов: {_processedCount}");
    }

    private void Produce(int producerId, int count)
    {
        for (int i = 1; i <= count; i++)
        {
            int value = producerId * 100 + i;

            _queue.Enqueue(value);

            Console.WriteLine(
                $"Производитель {producerId}: создал {value}");

            // Если убрать Set(), потребитель может остаться в ожидании.
            _signal.Set();

            Thread.Sleep(Random.Shared.Next(100, 400));
        }

        Console.WriteLine(
            $"Производитель {producerId}: завершил работу");
    }

    private void Consume()
    {
        while (true)
        {
            _signal.WaitOne();

            // Один сигнал может соответствовать нескольким элементам,
            // поэтому после пробуждения забираем всё из очереди.
            while (_queue.TryDequeue(out int value))
            {
                if (value == -1)
                {
                    Console.WriteLine("Потребитель завершает работу.");
                    return;
                }

                if (value > 250)
                {
                    Console.WriteLine("Высокоприоритетная задача");
                }

                Console.WriteLine($"Потребитель обработал: {value}");

                Interlocked.Increment(ref _processedCount);
                Thread.Sleep(ProcessingDelayMs);
            }
        }
    }

    public void Dispose()
    {
        _signal.Dispose();
    }
}

public class MutexDemo
{
    public static void Run()
    {
        Console.WriteLine("=== MUTEX DEMO ===");

        using Mutex mutex = new(
            initiallyOwned: false,
            name: "Local\\SynchronizationPrimitivesDemo");

        bool acquired = mutex.WaitOne(0);

        if (!acquired)
        {
            Console.WriteLine("Другой экземпляр приложения уже работает.");
            return;
        }

        try
        {
            Console.WriteLine("Блокировка получена. Ресурс занят.");
            Console.WriteLine("Нажмите Enter для освобождения блокировки.");
            Console.ReadLine();
        }
        finally
        {
            mutex.ReleaseMutex();
            Console.WriteLine("Блокировка освобождена.");
        }
    }
}

public class Counter
{
    private readonly object _lockObject = new();
    private int _value;

    public void Increment()
    {
        lock (_lockObject)
        {
            _value++;
        }
    }

    public int Value => _value;
}

public class InterlockedCounter
{
    private int _value;

    public void Increment()
    {
        Interlocked.Increment(ref _value);
    }

    public int Value => _value;
}

public static class CounterDemo
{
    public static async Task RunAsync()
    {
        const int taskCount = 10;
        const int incrementsPerTask = 10_000;

        var lockedCounter = new Counter();
        var interlockedCounter = new InterlockedCounter();

        Task[] lockTasks = Enumerable.Range(0, taskCount)
            .Select(_ => Task.Run(() =>
            {
                for (int i = 0; i < incrementsPerTask; i++)
                {
                    lockedCounter.Increment();
                }
            }))
            .ToArray();

        Task[] interlockedTasks = Enumerable.Range(0, taskCount)
            .Select(_ => Task.Run(() =>
            {
                for (int i = 0; i < incrementsPerTask; i++)
                {
                    interlockedCounter.Increment();
                }
            }))
            .ToArray();

        await Task.WhenAll(lockTasks);
        await Task.WhenAll(interlockedTasks);

        Console.WriteLine("=== COUNTER DEMO ===");
        Console.WriteLine($"Счётчик с lock:       {lockedCounter.Value}");
        Console.WriteLine($"Счётчик с Interlocked: {interlockedCounter.Value}");
        Console.WriteLine("Ожидаемый результат:   100000");
    }
}
