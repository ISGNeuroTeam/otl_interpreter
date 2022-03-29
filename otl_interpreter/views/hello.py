from rest.views import APIView
from rest.response import Response, status
from rest.permissions import AllowAny


class HelloView(APIView):
    permission_classes = (AllowAny, )

    def get(self, request):
        return Response(
            {
                'message': 'Hello',
            },
            status.HTTP_200_OK
        )


