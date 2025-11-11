package otus.homework.reactivecats

import android.content.Context
import androidx.lifecycle.LiveData
import androidx.lifecycle.MutableLiveData
import androidx.lifecycle.ViewModel
import androidx.lifecycle.ViewModelProvider
import io.reactivex.Flowable
import io.reactivex.Single
import io.reactivex.android.schedulers.AndroidSchedulers
import io.reactivex.disposables.CompositeDisposable
import retrofit2.HttpException
import java.lang.ref.WeakReference
import java.util.concurrent.TimeUnit

class CatsViewModel(
    val catsService: CatsService,
    val localCatFactsGenerator: LocalCatFactsGenerator,
    val context: WeakReference<Context>,
) : ViewModel() {

    private val _catsLiveData = MutableLiveData<Result>()
    val catsLiveData: LiveData<Result> = _catsLiveData

    private val compositeDisposable = CompositeDisposable()

    init {
        getFactsV1()
//        getFactsV2()
    }

    fun getFactsV1() {
        val disposable = Flowable.interval(2000, TimeUnit.MILLISECONDS)
            .flatMapSingle {
                catsService.getCatFact().onErrorResumeNext { error ->
                    if (error is HttpException) {
                        Single.error(error)
                    } else {
                        localCatFactsGenerator.generateCatFact()
                    }
                }
            }
            .distinctUntilChanged()
            .observeOn(AndroidSchedulers.mainThread())
            .subscribe(
                { fact ->
                    _catsLiveData.value = Success(fact)
                },
                { error ->
                    _catsLiveData.value = when (error) {
                        is HttpException -> {
                            Error(
                                error.message
                                    ?: context.get()?.getString(R.string.default_error_text)
                                    ?: ""
                            )
                        }

                        else -> ServerError
                    }
                }
            )

        compositeDisposable.add(disposable)
    }

    // Решил придумать кейс, где можно применить localCatFactsGenerator.generateCatFactPeriodically()
    // Логика похожая на V1, но теперь локал генератор 5 элементов берет, затем пробует еще раз через сервер
    fun getFactsV2() {
        val disposable = catsService.getCatFact()
            .toFlowable()
            .onErrorResumeNext { error: Throwable ->
                if (error is HttpException) {
                    Flowable.error(error)
                } else {
                    localCatFactsGenerator.generateCatFactPeriodically().take(5)
                }
            }
            .repeatWhen { handler -> handler.delay(2000, TimeUnit.MILLISECONDS) }
            .observeOn(AndroidSchedulers.mainThread())
            .subscribe(
                { fact ->
                    _catsLiveData.value = Success(fact)
                },
                { error ->
                    _catsLiveData.value = when (error) {
                        is HttpException -> {
                            Error(
                                error.message
                                    ?: context.get()?.getString(R.string.default_error_text)
                                    ?: ""
                            )
                        }

                        else -> ServerError
                    }
                }
            )

        compositeDisposable.add(disposable)
    }

    override fun onCleared() {
        compositeDisposable.clear()
    }
}

class CatsViewModelFactory(
    private val catsRepository: CatsService,
    private val localCatFactsGenerator: LocalCatFactsGenerator,
    private val context: Context
) :
    ViewModelProvider.NewInstanceFactory() {

    override fun <T : ViewModel> create(modelClass: Class<T>): T =
        CatsViewModel(catsRepository, localCatFactsGenerator, WeakReference(context)) as T
}

sealed class Result
data class Success(val fact: Fact) : Result()
data class Error(val message: String) : Result()
object ServerError : Result()