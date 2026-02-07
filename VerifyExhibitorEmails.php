<?php

namespace App\Jobs;

use App\Models\ExhibitorEmailQueue;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;

class VerifyExhibitorEmails implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    private int $maxApiAttempts = 3;
    private int $apiDelayMs = 150;
    private int $apiJitterMs = 80;

    public $timeout = 300;

    public function __construct(public string $edition)
    {
        $this->onQueue('email-verify');
    }

    public function handle(): void
    {
        $lockKey = 'mailnjoy_verify:' . $this->edition;

        $got = DB::selectOne('SELECT GET_LOCK(?, 0) AS l', [$lockKey])->l ?? 0;

        if ((int) $got !== 1) {
            return;
        }

        try
        {
            $rows = $this->reserveRows(20);

            if ($rows->isEmpty()) {
                return;
            }

            foreach ($rows as $i => $row)
            {
                try
                {
                    $ok = isEmailValid($row->email, true);

                    $row->status = $ok ? 'in_queue' : 'bad';
                    $row->save();
                }
                catch (\Throwable $e)
                {
                    $row->api_attempts = (int) ($row->api_attempts ?? 0) + 1;
                    $row->last_error = $e->getMessage();

                    $row->status = ($row->api_attempts >= $this->maxApiAttempts) ? 'canceled' : 'api_check';
                    $row->save();
                }
                finally
                {
                    // Micro-attente entre chaque appel API (évite les salves)
                    if ($i < ($rows->count() - 1)) {
                        $sleepMs = $this->apiDelayMs + random_int(0, $this->apiJitterMs);
                        usleep($sleepMs * 1000); // usleep = microsecondes
                    }
                }
            }

            $still = ExhibitorEmailQueue::query()->where('edition', $this->edition)->where('status', 'api_check')->exists();

            if ($still)
            {
                self::dispatch($this->edition)
                    ->onQueue('email-verify')
                    ->delay(now()->addSeconds(30));
            }
        }
        finally
        {
            DB::select('SELECT RELEASE_LOCK(?)', [$lockKey]);
        }
    }

    private function reserveRows(int $limit): Collection
    {
        return DB::transaction(function () use ($limit) {
            $rows = ExhibitorEmailQueue::query()
                ->where('edition', $this->edition)
                ->where('status', 'api_check')
                ->orderBy('id')
                ->limit($limit)
                ->lockForUpdate()
                ->get();

            if ($rows->isNotEmpty()) {
                ExhibitorEmailQueue::whereIn('id', $rows->pluck('id'))
                    ->update(['status' => 'api_processing']);

                $rows->each->refresh();
            }

            return $rows;
        }, 3);
    }
}
