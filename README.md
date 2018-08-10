# Spark ML Example - 협업필터링 (Collaborative Filtering)
9가지 사례로 익히는 고급 스파크 분석 (Advanced Analytics with Spark)
***

## 추천시스템 (Recommender System)

추천 시스템은 이미 산업 전반에 광범위하게 사용되고 있는 기술입니다. 최근에는 머신러닝을 통해 기존의 추천 시스템보다 정밀하고 개인화된 시스템으로 발전하고 있습니다.

(구글 학술 검색을 통한 'recommender systems' 키워드 검색 결과는 약12만건, 'collaborative filtering'은 약 1만7천건 정도 됩니다.)

가장 보편화된 협업 필터링만하더라도 1990년부터 활발히 연구되어 90년대후반부터 지금까지 다양한 학술 연구와 실제 성공 사례 (아마존, 넷플릭스, 페이스북, 인스타그램 등등)로 이어지고 있습니다. 추천 시스템은 이미 오래 전부터 통계적/경험적 접근법으로 검증된 많은 기법들이 있습니다. 다만 다양한 산업 도메인의 통계/수학 전문가들이 연구한 모델들이 자원(저장소, 컴퓨팅)등의 제한에 묶여있었던 것이 IT기술의 발달과 폭팔적인 데이터의 증가를 통해 과거의 한계를 넘어섰고 연쇄적으로 새로운 시도와 접근법들이 나오며 발전하고 있습니다.

과거의 추천 시스템이 전체 데이터를 대상으로 일반화하여 확률을 높였다면 최근의 추천 시스템은 기존 일반화된 데이터 + 개인 특화된 데이터를 결합하는 방향으로 가고 있습니다.
또한 쇼핑 / 뉴스 사람의 영이 아닌 공장 / 시스템등 위험 방지하고 생산성의 질을 높이는 분야에도 적용되고 있습니다.

추천 시스템은 여러가지 새로운 기술을 기반으로하지만, **협업 필터링 (Collaborative filtering)**과 **콘텐츠 기반 필터링 (Content-based filtering)**을 기반으로 이루어져 있는 경우가 많습니다.

예제 프로젝트를 통해 협업필터링의 한종류인 **교차최소제곱 (Alternating Least Squares)**알고리즘을 기반으로한 추천시스템이 어떻게 구성되는지 간략히 알아보고 응용할 수 있는 배경 지식을 알아 봅니다.

### 추천 시스템이란

- [위키 백과 (원문)](https://en.wikipedia.org/wiki/Recommender_system)
> A recommender system or a recommendation system (sometimes replacing "system" with a synonym such as platform or engine) is a subclass of information filtering system that seeks to predict the "rating" or "preference" a user would give to an item.

- [위키 백과](https://ko.wikipedia.org/wiki/%EC%B6%94%EC%B2%9C_%EC%8B%9C%EC%8A%A4%ED%85%9C)
> 추천 시스템은 정보 필터링 기술의 일종으로, 특정 사용자가 관심을 가질만한 정보 (영화, 음악, 책, 뉴스, 이미지, 웹 페이지 등)를 추천하는 것이다. 추천 시스템에는 협업 필터링 기법을 주로 사용한다. 소셜 북마크 사이트에서 링크를 사람들에게 추천하고 무비렌즈 데이터 세트에서 영화를 추천하는 방법등이 이에 속한다.

- 컨텐츠 징흥원의 추천 알고리즘 기고 ["컨텐츠 추천 알고리즘의 진화""](http://www.kocca.kr/insight/vol05/vol05_04.pdf)
- 정보통신 산업진흥원 추천 시스템 기고 ["Collaborative Filtering - 추천시스템의 핵심 기술""](http://www.oss.kr/info_techtip/show/5419f4f9-12a1-4866-a713-6c07fd36e647)

### 협업필터링 (Collavorative Filtering)

> 협업 필터링(collaborative filtering)은 많은 사용자들로부터 얻은 기호정보(taste information)에 따라 사용자들의 관심사들을 자동적으로 예측하게 해주는 방법이다. 협력 필터링 접근법의 근본적인 가정은 사용자들의 과거의 경향이 미래에서도 그대로 유지 될 것이라는 전제에 있다. 예를 들어, 음악에 관한 협력 필터링 혹은 추천시스템(recommendation system)은 사용자들의 기호(좋음, 싫음)에 대한 부분적인 목록(partial list)을 이용하여 그 사용자의 음악에 대한 기호를 예측하게 된다. 이 시스템은 특정 사용자의 정보에만 국한 된 것이 아니라 많은 사용자들로부터 수집한 정보를 사용한다는 것이 특징이다. 이것이 단순히 투표를 한 수를 기반으로 각 아이템의 관심사에 대한 평균적인 평가로 처리하는 방법과 차별화 된 것이다. 즉 고객들의 선호도와 관심 표현을 바탕으로 선호도, 관심에서 비슷한 패턴을 가진 고객들을 식별해 내는 기법이다. 비슷한 취향을 가진 고객들에게 서로 아직 구매하지 않은 상품들은 교차 추천하거나 분류된 고객의 취향이나 생활 형태에 따라 관련 상품을 추천하는 형태의 서비스를 제공하기 위해 사용된다. 출처 : [위키백과](https://ko.wikipedia.org/wiki/%ED%98%91%EC%97%85_%ED%95%84%ED%84%B0%EB%A7%81)

[![https://en.wikipedia.org/wiki/Collaborative_filtering](https://upload.wikimedia.org/wikipedia/commons/thumb/5/52/Collaborative_filtering.gif/300px-Collaborative_filtering.gif)

이미지 출처 : [위키백과](https://en.wikipedia.org/wiki/Collaborative_filtering)

협업 필터링의 접근법은 먼저 많은 사용자로부터 선호도 또는 취향에 대한 다양한 의견을 수집합니다.
그리고 문제 A에 대해 동일한 의견을 갖는 사람은 다른 문제 B에 대해 동일한 의견을 가질 가능성이 높다는 전제를 가지고 접근하는 것이 협업 필터링의 기본 접근법입니다.
사람뿐만 아니라 다양한 유/무형의 데이터 소스 간의 정보 또는 패턴을 필터링하는 과정 역시 협업 필터링이라고 볼수 있습니다. 집안의 센서들간의 정보, 대규모 공장 기계간의 정보 또는 기계 부품들 센서간의 정보 역시 협업 필터링의 대상이 될 수 있습니다.

이러한 접근법에 따라 A 온라인 몰의 20대 회원 2명이 단지 20대라는 이유로 유사한 성향으로 분류하는 것은 협업 필터링의 결과물이라고 볼 수 없습니다.
두 회원이 구매한 물건중 공통되는 것이 많기 때문에 A와 B는 유사한 성향이라고 결정하는 것이 바로 협업 필터링의 단순한 예라고 볼 수 있습니다.

협업 필터링은 **다수의 사용자와 다수의 상품 사이에 존재 하는 상호작용 (구매 빈도, 선호도, 평점 등)을 이용하여 상호작용이 존재하지 않는 상품의 관계를 설명**하고자 할때 유용한 모델입니다.

### 교차최소제곱법 (The Alternating Least Squares Recommender Algorithm)

협업필터링을 풀어내는 방법에는 다양한 알고리즘이 존재하는데 Spark MLlib에서는 행렬 분해(Matrix Factorization)를 이용한 대수적 방법을 이용해 풀어내는 교차최소제곱법(ALS)을 제공하고 있습니다.

교차최소제곱법은 행렬 분해를 이용하여 협업필터링을 구현하는 알고리즘 중 하나 입니다. 사용자와 상품 A의 관계를 행렬 A(사용자 * 상품)로 보고 행렬 A를 작은 행렬 X, Y의 행렬 곱으로 분해하여 이 둘을 만족하는 최적의 해를 구하는 원리입니다. 이런 종류의 접근법은 과거 넷플리스가 자사의 컨텐츠를 추천하는 알고리즘 대회를 열던 시절 두각을 나타낸 [암묵적 피드백 데이터넷에 대한 협업 필터링(Collaborative Filtering for Implicit Feedback Datasets)](http://yifanhu.net/PUB/cf.pdf)과 [넷플리스 프라이즈를 위한 대규모의 병렬 협업 필터링(Large-scale Parallel Collaborative Filtering for the Netflix Prize)](http://www.grappa.univ-lille3.fr/~mary/cours/stats/centrale/reco/paper/MatrixFactorizationALS.pdf)과 같은 논문을 통해 널리 사용하게 되었습니다. Spark MLlib의 ALS모델은 이 논문들의 아이디어를 Spark Cluster상에 구현한 것입니다.

ALS알고리즘은 간단하고 최적화된 선형대수 기법과 병렬 처리가 가능한 점 덕분에 매우 큰 규모에서도 빠르게 데이터를 처리할 수 있는 장점을 가지고 있습니다.

### DataSet

데이터셋 다운로드 : [profiledata_06-May-2005.tar.gz](https://storage.googleapis.com/aas-data-sets/profiledata_06-May-2005.tar.gz)

    - user_artist_data.txt (407M) : 사용자ID, 아티스트ID, 재생 횟수
    1000002 1 55
    1000002 1000006 33
    1000002 1000007 8
    1000002 1000009 144
    1000002 1000010 314
    ...

    - artist_data.txt (54M) : 아티스트ID, 아티스트이름
    1134999	Crazy Life
    6821360	Pang Nakarin
    10113088	Terfel, Bartoli- Mozart: Don
    10151459	The Flaming Sidebur
    6826647	Bodenstandig 3000
    ...
    
    - artist_alias.txt (2.8M) : 잘못 알려진 아티스트ID, 올바른 아티스트ID
    1092764	1000311
    1095122	1000557
    6708070	1007267
    10088054	1042317
    1195917	1042317
    ...

### Run

데이터셋을 다운 받아 다음 경로에 위치시켜야 합니다. : tmp/sample_data/

    mv ${project_home}

    mkdir -p tmp/sample_data      
    
    wget https://storage.googleapis.com/aas-data-sets/profiledata_06-May-2005.tar.gz

    tar -xvf profiledata_06-May-2005.tar.gz

    mv profiledata_06-May-2005.tar.gz/* tmp/sample_data

    rm profiledata_06-May-2005.tar.gz
    
    ./gradlew task main

